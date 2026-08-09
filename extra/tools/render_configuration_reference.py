#!/usr/bin/env python
import inspect
import sys
from typing import Any, Iterator, List, Type
from faust.types.settings import Settings
from faust.types.settings.params import Param
from faust.types.settings.sections import Section

SECTION_TEMPLATE = '''\
.. _{section.refid}:

{title}

{settings}
'''

SETTING_TEMPLATE = '''\
.. setting:: {setting.name}

{title}

{metadata}

{content}

'''


class Rst:

    def public_module(self, t: Type) -> str:
        """The importable module a type should be referenced through.

        ``__module__`` is not always the name the documentation uses.  Python
        3.13 moved :class:`pathlib.Path` into ``pathlib._local``, so
        ``__module__`` became a private submodule and the rendered
        ``:class:`~pathlib._local.Path``` resolved nowhere -- and the reference
        changed depending on which interpreter ran the generator.

        Walk up the private components and take the shallowest package that
        still exposes the very same object, so the reference stays public and
        the output stays identical across versions.
        """
        parts = t.__module__.split('.')
        while len(parts) > 1 and parts[-1].startswith('_'):
            parts.pop()
            candidate = '.'.join(parts)
            module = sys.modules.get(candidate)
            if module is not None and getattr(module, t.__name__, None) is t:
                return candidate
        return t.__module__

    def to_ref(self, t: Type) -> str:
        name: str
        module = self.public_module(t)
        if module == 'builtins':
            return self._class(t.__name__)
        elif module == 'typing':
            if t is Any:
                name = 'Any'
            else:
                name = getattr(t, '_name', None) or t.__name__
                if name == 'List':
                    list_type = t.__args__ and t.__args__[0] or Any
                    return ' '.join([
                        self.literal('['),
                        self.to_ref(list_type),
                        self.literal(']'),
                    ])
                elif name.startswith(('Dict', 'Mapping', 'MutableMapping')):
                    key_type = value_type = Any
                    if t.__args__:
                        key_type = t.__args__[0]
                    if len(t.__args__) > 1:
                        value_type = t.__args__[1]
                    return ' '.join([
                        self.literal('{'),
                        ': '.join([
                            self.to_ref(key_type),
                            self.to_ref(value_type),
                        ]),
                        self.literal('}'),
                    ])
        else:
            name = t.__name__

        return self._class(f'{module}.{name}')

    def header(self, sep: str, title: str) -> str:
        return '\n'.join([title, sep * len(title)])

    def header1(self, title: str) -> str:
        return self.header('=', title)

    def header2(self, title: str) -> str:
        return self.header('-', title)

    def header3(self, title: str) -> str:
        return self.header('~', title)

    def header4(self, title: str) -> str:
        return self.header('^', title)

    def ref(self, ref_class: str, value: str) -> str:
        return f':{ref_class}:`{value}`'

    def envvar(self, name: str) -> str:
        return self.ref('envvar', name)

    def const(self, value: str) -> str:
        return self.ref('const', value)

    def _class(self, value: str) -> str:
        if '.' in value:
            value = '~' + value
        return self.ref('class', value)

    def option(self, value: str) -> str:
        return self.ref('option', value)

    def literal(self, s: str) -> str:
        return f'``{s}``'

    def directive(self, name: str, value: str, content: str = None) -> str:
        res = f'.. {name}:: {value}\n'
        if content is not None:
            res += '\n' + self.reindent(8, content) + '\n'
        return res

    def inforow(self, name: str, value: str) -> str:
        return f':{name}: {value}'

    def normalize_docstring_indent(self, text: str) -> str:
        """Dedent a docstring, leaving relative indentation intact.

        :func:`inspect.cleandoc` rather than the hand-rolled scan this used to
        do, which was wrong in two ways.

        It looked for the first *indented* line after the summary and stripped
        that much from every line.  On Python 3.12 and older that happened to
        be the body indent, so it worked.  Python 3.13 strips the common
        leading whitespace from docstrings at compile time, so by the time the
        scan runs the body is already flush left and the first indented line it
        finds is the body of a ``.. warning::`` or ``.. note::`` -- whose
        indent it then removed, breaking the directive: the content escaped the
        admonition and rendered as ordinary paragraphs.

        It also stripped one character too many (``i > n`` where ``i >= n`` was
        meant), which is why directive bodies in the committed reference sit at
        three spaces rather than four.

        ``cleandoc`` dedents by the *minimum* indent instead, so it is a no-op
        on an already-dedented docstring and produces byte-identical output on
        every supported interpreter.
        """
        return inspect.cleandoc(text)

    def reindent(self, new_indent: int, text: str) -> str:
        return '\n'.join(
            ' ' * new_indent + line
            for line in self.normalize_docstring_indent(text).splitlines()
        )


class ConfigRef(Rst):

    def section(self, section: Section, settings: List[Param]) -> str:
        return SECTION_TEMPLATE.format(
            section=section,
            title=self.header1(section.title),
            settings=''.join(
                self.setting(setting) for setting in settings
                if not setting.deprecated
            ),
        )

    def setting(self, setting: Param) -> str:
        return SETTING_TEMPLATE.format(
            setting=setting,
            title=self.header2(self.literal(setting.name)),
            content=self.normalize_docstring_indent(setting.__doc__),
            metadata='\n'.join(self.setting_metadata(setting)),
        )

    def setting_default(self, default_value: None) -> str:
        if default_value is None:
            return self.const('None')
        elif default_value is True:
            return self.const('True')
        elif default_value is False:
            return self.const('False')
        return self.literal(repr(default_value))

    def setting_metadata(self, setting: Param) -> Iterator[str]:
        if setting.version_introduced:
            yield self.directive('versionadded', setting.version_introduced)
        if setting.version_changed:
            for version, reason in setting.version_changed.items():
                yield self.directive('versionchanged', version, reason)
        yield self.inforow('type', ' / '.join(
            self.to_ref(t) for t in setting.text_type))

        if setting.default_template:
            default_info_title = 'default (template)'
            default_value = self.setting_default(setting.default_template)
        elif setting.default_alias:
            default_info_title = 'default (alias to setting)'
            default_value = self.settingref(setting.default_alias)
        else:
            default_info_title = 'default'
            default_value = self.setting_default(setting.default)
        yield self.inforow(default_info_title, default_value)

        if setting.env_name:
            yield self.inforow(
                'environment', self.envvar(setting.env_name))

        if setting.related_cli_options:
            yield self.inforow(
                'related-command-options',
                ', '.join(
                    self.option(f'{command} {opt}')
                    for command, opts in setting.related_cli_options.items()
                    for opt in opts
                ),
            )
        if setting.related_settings:
            yield self.inforow(
                'related-settings',
                ', '.join(
                    self.settingref(setting.name)
                    for setting in setting.related_settings
                ),
            )

    def settingref(self, setting: str) -> str:
        return self.ref('setting', setting)


def render(fh=sys.stdout):
    configref = ConfigRef()
    for section, settings in Settings.SETTINGS_BY_SECTION.items():
        print(configref.section(section, settings), file=fh, end='')


if __name__ == '__main__':
    render()
