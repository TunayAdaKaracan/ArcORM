import asyncpg
import asyncio
from .Modal import Modal


class PostgreEngine:
    CONNECTIONS: [str, asyncpg.Pool] = {}

    def __init__(self, user, password, host):
        self.tables: [Modal] = []

        self.user = user
        self.password = password
        self.host = host

    async def register_modal(self, modal: Modal):
        modal._init_register()

        self.tables.append(modal)
        if modal.get_database() not in PostgreEngine.CONNECTIONS:
            print("creating a pool connection")
            PostgreEngine.CONNECTIONS[modal.get_database()] = await asyncpg.create_pool(user=self.user, password=self.password, host=self.host, database=modal.get_database(),  max_inactive_connection_lifetime=100)

        async with PostgreEngine.CONNECTIONS[modal.get_database()].acquire() as con:
            await con.execute(modal.initialize_table())

        print(modal.initialize_table())

        modal._create_object_handler(PostgreEngine.CONNECTIONS[modal.get_database()])