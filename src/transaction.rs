#![forbid(unsafe_code)]

use std::ops::Deref;
use std::{
    any::Any,
    cell::{Ref, RefCell, RefMut},
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    rc::Rc,
};

use crate::object::Store;
use crate::{
    data::ObjectId,
    error::{Error, NotFoundError, Result},
    object::{Object, Schema},
    storage::StorageTransaction,
};

////////////////////////////////////////////////////////////////////////////////

// TODO: your code goes here.
pub struct Transaction<'a> {
    inner: Box<dyn StorageTransaction + 'a>,
    objects: RefCell<HashMap<(&'static Schema, ObjectId), TxState>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(inner: Box<dyn StorageTransaction + 'a>) -> Self {
        Self {
            inner,
            objects: RefCell::new(HashMap::new()),
        }
    }

    fn ensure_table<T: Object>(&self) -> Result<()> {
        let exists = self.inner.table_exists(T::schema().table_name)?;
        if !exists {
            self.inner.create_table(T::schema())
        } else {
            Ok(())
        }
    }

    pub fn create<T: Object>(&self, src_obj: T) -> Result<Tx<'_, T>> {
        self.ensure_table::<T>()?;
        let schema = T::schema();
        let id = self.inner.insert_row(schema, &src_obj.to_row())?;
        let state = TxState {
            id,
            obj: Rc::new(RefCell::new(src_obj)),
            state: Rc::new(RefCell::new(ObjectState::Clean)),
        };
        self.objects
            .borrow_mut()
            .insert((T::schema(), id), state.clone());
        Ok(Tx::new(state))
    }

    pub fn get<T: Object>(&self, id: ObjectId) -> Result<Tx<'_, T>> {
        self.ensure_table::<T>()?;
        match self.objects.borrow_mut().entry((T::schema(), id)) {
            Entry::Vacant(place) => {
                let row = self.inner.select_row(id, T::schema())?;
                let state = TxState {
                    id,
                    obj: Rc::new(RefCell::new(T::from_row(row))),
                    state: Rc::new(RefCell::new(ObjectState::Clean)),
                };
                let tx = Tx::new(state.clone());
                place.insert(state);
                Ok(tx)
            }
            Entry::Occupied(e) => {
                let rc = e.get().clone();
                if rc.state.borrow().deref() == &ObjectState::Removed {
                    return Err(Error::NotFound(Box::new(NotFoundError {
                        object_id: id,
                        type_name: T::schema().type_name,
                    })));
                }
                if !rc.obj.borrow().as_any().is::<T>() {
                    panic!("type mismatch")
                }
                Ok(Tx::new(rc))
            }
        }
    }

    fn try_apply(&self) -> Result<()> {
        for ((schema, id), obj) in self.objects.borrow().iter() {
            let state = obj.state.borrow();
            match state.deref() {
                ObjectState::Modified => {
                    self.inner
                        .update_row(*id, schema, &obj.obj.borrow().to_row())?;
                }
                ObjectState::Removed => {
                    self.inner.delete_row(*id, schema)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.try_apply()?;
        self.inner.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.objects.borrow_mut().iter().for_each(|(_, obj)| {
            *obj.state
                .try_borrow_mut()
                .expect("cannot rollback with borrowed values") = ObjectState::Clean;
        });
        self.objects.borrow_mut().clear();
        self.inner.rollback()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    Clean,
    Modified,
    Removed,
}

#[derive(Clone)]
struct TxState {
    id: ObjectId,
    obj: Rc<RefCell<dyn Store>>,
    state: Rc<RefCell<ObjectState>>,
}

#[derive(Clone)]
pub struct Tx<'a, T: ?Sized> {
    state: TxState,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Any> Tx<'a, T> {
    fn new(state: TxState) -> Self {
        Self {
            state,
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> ObjectId {
        self.state.id
    }

    pub fn state(&self) -> ObjectState {
        *self.state.state.borrow()
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        if self.state() == ObjectState::Removed {
            panic!("cannot borrow a removed object")
        }
        Ref::map(self.state.obj.borrow(), |x| {
            x.as_any().downcast_ref::<T>().unwrap()
        })
    }

    pub fn borrow_mut(&self) -> RefMut<'_, T> {
        *self.state.state.borrow_mut() = ObjectState::Modified;
        RefMut::map(self.state.obj.borrow_mut(), |x| {
            x.as_mut_any().downcast_mut::<T>().unwrap()
        })
    }

    pub fn delete(self) {
        self.state
            .obj
            .try_borrow_mut()
            .expect("cannot delete a borrowed object");
        *self.state.state.borrow_mut() = ObjectState::Removed;
    }
}
