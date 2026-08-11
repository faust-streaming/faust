# cython: language_level=3
"""Cython optimized field descriptor."""


cdef class FieldDescriptorBase:
    """Read path of :class:`faust.models.fields.FieldDescriptor`.

    Only ``__get__`` lives here, because that is what runs on every access
    to every field of every model instance.  Everything else stays in the
    Python class, where it is readable and overridable.

    The four attributes ``__get__`` touches are declared on the extension
    type so it can reach them as C struct members instead of going through
    a dict lookup.  Every other attribute is an ordinary Python attribute
    in ``__dict__``, so nothing else about the class changes -- including
    ``as_dict()``/``clone()`` and tests that set attributes directly.
    """

    cdef public str field
    cdef public bint required
    cdef public bint lazy_coercion
    cdef public object _to_python
    cdef dict __dict__

    def __get__(self, instance, owner):
        cdef:
            dict instance_dict
            str field
            object to_python
            object value
            object evaluated_fields

        # Class attribute access: `Model.field` returns the descriptor.
        if instance is None:
            return self

        field = self.field
        instance_dict = instance.__dict__
        to_python = self._to_python
        value = instance_dict[field]
        if self.lazy_coercion and to_python is not None:
            evaluated_fields = instance.__evaluated_fields__
            if field not in evaluated_fields:
                if value is not None or self.required:
                    value = instance_dict[field] = to_python(value)
                evaluated_fields.add(field)
        return value
