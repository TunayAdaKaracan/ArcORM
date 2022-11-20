import typing
import asyncpg
from .Types import Type
from .Exceptions import NoMetadataException, DatabaseAndTablenameFieldCanNotBeNull, ModalCanBeInitializedOnce


class ObjectHandler:
    def __init__(self, pool: asyncpg.Pool, modal):
        self.__pool = pool
        self.__modal: Modal = modal

    async def create(self, **kwargs):
        required = {}
        for k, v in kwargs.items():
            if k in self.__modal.get_columns():
                required[k] = v

        execute_str = ""
        execute_str += "insert into "
        execute_str += self.__modal.get_tablename()
        execute_str += "("
        for k in required.keys():
            execute_str += k + ","
        execute_str = execute_str[:-1]
        execute_str += ")"
        execute_str += " values"
        execute_str += "("
        index = 1
        for _ in required.values():
            execute_str += "$"+str(index)+","
            index += 1
        execute_str = execute_str[:-1]
        execute_str += ")"

        async with self.__pool.acquire() as con:
            await con.execute(execute_str, *required.values())

        return await self.findOne(**required)

    async def findOne(self, **kwargs):
        execute_str = "select * from "+self.__modal.get_tablename()
        execute_str += " where "
        index = 1
        for k in kwargs.keys():
            execute_str += k
            execute_str += "="
            execute_str += "$"+str(index)
            execute_str += " and "
            index += 1
        execute_str = execute_str[:-4]

        async with self.__pool.acquire() as con:
            result = await con.fetchrow(execute_str, *kwargs.values())
            if result is None:
                return None

            modal = self.__modal.__new__(self.__modal.__class__)
            dct = {}
            for f, v in result.items():
                dct[str(f)] = v
            modal._load_value(**dct)
            return modal

    async def findMany(self, **kwargs):
        execute_str = "select * from "+self.__modal.get_tablename()
        if(len(kwargs) != 0):
            execute_str += " where "
            index = 1
            for k in kwargs.keys():
                execute_str += k
                execute_str += "="
                execute_str += "$"+str(index)
                execute_str += " and "
                index += 1
            execute_str = execute_str[:-4]

        async with self.__pool.acquire() as con:
            results = await con.fetch(execute_str, *kwargs.values())
            if results is None or len(results) == 0:
                return []
            modals = []
            for r in results:
                modal = self.__modal.__new__(self.__modal.__class__)
                dct = {}
                for f, v in r.items():
                    dct[str(f)] = v
                modal._load_value(**dct)
                modals.append(modal)
            return modals


class Modal:
    objects: typing.Union[None, ObjectHandler] = None

    def _load_value(self, **kwargs):
        for k, v in kwargs.items():
            self.__dict__[k] = v

    def _init_register(self):
        self.__meta = getattr(self, "Meta", None)
        if self.__meta is None:
            raise NoMetadataException("Meta is null")

        if not all(name in dir(self.__meta) for name in ["database", "tablename"]):
            raise DatabaseAndTablenameFieldCanNotBeNull()

        self.__database = getattr(self.__meta, "database")
        self.__table_name = getattr(self.__meta, "tablename")

        self.__columns: [str, Type] = {}
        for prt in dir(self):
            if prt.startswith("__") and prt.endswith("__"):
                continue
            if not isinstance(getattr(self, prt), Type):
                continue
            self.__columns[prt] = getattr(self, prt)

    def _create_object_handler(self, pool: asyncpg.Pool):
        if Modal.objects is not None:
            raise ModalCanBeInitializedOnce("A modal only can be initialized once.")
        Modal.objects = ObjectHandler(pool, self)

    def initialize_table(self):
        table = f"CREATE TABLE IF NOT EXISTS {self.__table_name}("
        for column_name, column_val in self.__columns.items():
            col: Type = column_val
            table += column_name + " " + col.initialize() + ","
        table = table[:-1]
        table += ");"
        return table

    def get_columns(self):
        return self.__columns.copy()

    def get_database(self):
        return self.__database

    def get_tablename(self):
        return self.__table_name

    def get_meta(self):
        return self.__meta

    def __repr__(self):
        label = "<ModalObject "
        for column_name, column_val in self.__dict__.items():
            label += column_name + "=" + str(column_val) + ", "
        label = label[:-2]
        label += ">"
        return label