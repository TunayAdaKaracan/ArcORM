import asyncio
from orm.Engine import PostgreEngine
from orm.Modal import Modal
from orm.Types import Integer, Text, Serial


class Book(Modal):
    class Meta:
        database = "panvatoz"
        tablename = "books"

    id = Serial(primary=True, non_null=True)
    title = Text(non_null=True, default="Not found")


async def main():
    engine = PostgreEngine("postgres", "kutup", "127.0.0.1")
    await engine.register_modal(Book())
    book = await Book.objects.findMany()
    print(book)



if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())