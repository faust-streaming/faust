Django and Faust
================

ASGI co-hosting
---------------

The Django and Faust applications can run together on one asyncio event loop::

    $ uvicorn proj.asgi:application

``proj/asgi.py`` wraps Django's standard ``get_asgi_application()`` callable
with ``FaustLifespanMiddleware``. The wrapper starts Faust before the ASGI
server accepts requests and stops it during ASGI shutdown. HTTP and WebSocket
scopes are still handled entirely by Django.

The separate Faust worker entry point remains available when Django and Faust
should run as different processes::

    $ proj-faust worker -l info

Directory layout
----------------

- ``proj/``

  This is the main Django project.

  We have also added a ``proj/__main__.py`` that executes if you do
  ``python -m proj``, and it will work as the manage.py for the project.

  This is also installed by setup.py as an entry point, so after
  ``python setup.py install`` or ``python setup.py develop`` the
  ``proj`` command will be available::

        $ python setup.py develop
        $ proj runserver

    The above is the same as running ``manage.py runserver``, but it will
    be installed in the system path.

- ``faustapp/``

    This is a Django App that defines the Faust app used by the project,
    and it also configures Faust using Django settings.

    This faustapp is also installed by setup.py as the ``proj-faust`` program,
    and can be used to start a Faust worker for your Django project by doing::

        $ python setup.py develop
        $ proj-faust worker -l info

- ``accounts/``

    This is an example Django App with stream processors.
