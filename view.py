from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.ext import compiler

# Code adapted from: https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/Views

class CreateView(DDLElement):
    def __init__(self, name, selectable, prefixes=[]):
        self.name = name
        self.prefixes = prefixes
        self.selectable = selectable

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name

@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return "CREATE %s VIEW %s AS %s" % (
        " ".join(element.prefixes),
        element.name, 
        compiler.sql_compiler.process(element.selectable, literal_binds=True))

@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)
    
def View(name, metadata, selectable, prefixes=[]):
    t = table(name)

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, selectable, prefixes).execute_at('after-create', metadata)
    DropView(name).execute_at('before-drop', metadata)
    return t

