import sys
import random
import asyncpg
import asyncio
import http.server
import socketserver

postgresql_connection = "Fill this with your connection details"

initialize_shortener_sequence = [
    """
CREATE SEQUENCE public.shortener_short_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;
    """,
    """
ALTER SEQUENCE public.shortener_short_id_seq
    OWNER TO gel;
    """,
]  # Sequences for days

initialize_shortener_table = [
    """CREATE TABLE public.shortener
(
    id_code bigint NOT NULL DEFAULT nextval('shortener_short_id_seq'::regclass),
    url text NOT NULL,
    CONSTRAINT shortener_pkey PRIMARY KEY (id_code, url)
)
TABLESPACE pg_default""",
    """ALTER TABLE public.shortener
    OWNER to gel""",
    """
ALTER TABLE public.shortener
    ADD CONSTRAINT url_uniqueness UNIQUE (url);
    """,
]  # Tables for days


random.seed()

"""Absolutely and without a doubt DO NOT RUN THIS WHERE PEOPLE CAN CONNECT TO IT DIRECTLY

You have been warned.
—Damaged"""


PORT = 8000
domain = "http://192.168.56.101:8000"  # set this to your actual domain
url_prefix = "s"  # configure nginx (or similar) to redirect anything with the prefix to this port
url_form = "{domain}/{url_prefix}/{id}"
alphabet = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_~!$&'()*+,;=:@"
)
# /\ biggest alphabet is best alphabet! base79!


class Redirect:
    def __init__(self, url: str = None, short_url: str = None):
        if not (url is None) != (short_url is None):
            raise ValueError(
                f"You need to pass either <url> ({url}) or <short_url> ({short_url}) but not both."
            )
        self._url = url
        self._short_url = short_url
        self.conn = None
        self.ready = False
        self.built = False
        asyncio.create_task(self.build())

    async def wait_until_ready(self):
        await self.connect()
        for x in range(1, 10):
            if not self.ready:
                await asyncio.sleep(0.25 * x)
                continue
            return
        raise TimeoutError("Unable to connect/set up SQL")

    async def connect(self):
        self.conn = await asyncpg.connect(postgresql_connection)
        await self.check_table()
        self.ready = True

    async def disconnect(self):
        await self.conn.close()

    async def check_table(self):
        output = await self.conn.fetch(
            """SELECT EXISTS(
                SELECT *
                FROM information_schema.sequences
                WHERE sequence_name = 'shortener_short_id_seq'
            )"""
        )
        if not output[0]["exists"]:
            await self.initialize_sql_sequence()
        output = await self.conn.fetch(
            """SELECT EXISTS(
                SELECT *
                FROM information_schema.tables
                WHERE table_name = 'shortener'
            )"""
        )
        if not output[0]["exists"]:
            await self.initialize_sql_table()

    async def initialize_sql_table(self):
        for sql_command in initialize_shortener_table:
            await self.conn.execute(sql_command)

    async def initialize_sql_sequence(self):
        for sql_command in initialize_shortener_sequence:
            await self.conn.execute(sql_command)

    async def build(self):
        await self.wait_until_ready()
        if self._short_url:
            self._url = await self.fetch_url(self._short_url)
        elif self._url:
            self._short_url = await self.create_short_url(self._url)
        self.built = True
        await self.disconnect()

    async def url(self):
        while not self.built:
            await asyncio.sleep(0.1)
        return self._url

    async def short_url(self):
        while not self.built:
            await asyncio.sleep(0.1)
        return self._short_url

    async def fetch_url(self, short_url: str = None):
        id_code = short_url.rsplit("/", 1)[1]
        return await self.get_url_from_db(id_code)

    async def create_short_url(self, url: str = None):
        id_code = await self.insert_into_db(url)
        return url_form.format(id=encode(id_code), url_prefix=url_prefix, domain=domain)

    async def insert_into_db(self, url: str = None):
        if url is None:
            raise ValueError("Must set a value for <url>")
        output = await self.conn.fetch(
            """INSERT INTO public.shortener (url) VALUES ($1)
            ON CONFLICT
            DO NOTHING
            RETURNING id_code""",
            str(url),
        )
        if len(output) == 0:
            output = await self.conn.fetch(
                "SELECT id_code FROM public.shortener WHERE url = $1",
                url,
            )
        return int(output[0]["id_code"])

    async def get_url_from_db(self, id_code: str = None):
        if id_code is None:
            raise ValueError("Must set a value for <id_code>")
        output = await self.conn.fetch(
            "SELECT url FROM public.shortener WHERE id_code = $1",
            decode(id_code),
        )
        if len(output) == 0:
            raise RuntimeError("<id_code> ({id_code}) not found in DB")
        return output[0]["url"]


class Server(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            id_code = self.path.rsplit("/", 1)[1]
            url = asyncio.get_event_loop().run_until_complete(get_url_from_db(id_code))
            self.send_response(301)
            self.send_header("Location", url)
            self.end_headers()
            self.log_request(f"Sent user to {url} from {self.path.rsplit('/', 1)[1]}")
        except RuntimeError:
            self.send_response(404)
            self.end_headers()
            self.log_request(
                f"Sent user ({self.client_address}) a 404 from {self.path.rsplit('/', 1)[1]}"
            )
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.log_error("Something bad happened and I sent a 500!")
            self.log_error(str(e))


async def get_url_from_db(id_code: str = None):
    if id_code is None:
        raise ValueError("Must set a value for <id_code>")
    conn = await connect()
    output = await conn.fetch(
        "SELECT url FROM public.shortener WHERE id_code = $1",
        decode(id_code),
    )
    await disconnect(conn)
    if len(output) == 0:
        raise RuntimeError("<id_code> ({id_code}) not found in DB")
    return output[0]["url"]


async def connect():
    conn = await asyncpg.connect(postgresql_connection)
    await check_table(conn)
    return conn


async def disconnect(conn):
    await conn.close()


async def check_table(conn):
    output = await conn.fetch(
        """SELECT EXISTS(
            SELECT *
            FROM information_schema.sequences
            WHERE sequence_name = 'shortener_short_id_seq'
        )"""
    )
    if not output[0]["exists"]:
        await initialize_sql_sequence()
    output = await conn.fetch(
        """SELECT EXISTS(
            SELECT *
            FROM information_schema.tables
            WHERE table_name = 'shortener'
        )"""
    )
    if not output[0]["exists"]:
        await initialize_sql_table(conn)


async def initialize_sql_sequence(conn):
    for sql_command in initialize_shortener_sequence:
        await conn.execute(sql_command)


async def initialize_sql_table(conn):
    for sql_command in initialize_shortener_table:
        await conn.execute(sql_command)


def run_server():
    handler = Server
    try:
        with socketserver.TCPServer(("127.0.0.1", PORT), handler) as httpd:
            print("Serving at port: ", PORT)
            httpd.serve_forever()
    except Exception as e:
        print(e)
    del handler


def encode(number: int = None):
    if number is None:
        raise ValueError("Must set a value for <number>")
    if number < 0:
        raise ValueError(f"<number> ({number}) must be positive")
    out_num = []
    while number:
        number, i = divmod(number, len(alphabet))
        out_num.append(alphabet[i])
    return "".join(reversed(out_num))


def decode(id_code: str = None):
    if id_code is None:
        raise ValueError("Must set a value for <number>")
    output = 0
    base = len(alphabet)
    for character in id_code:
        value = alphabet.index(character)
        output *= base
        output += value
    return output


async def main():
    # This is an example of how to generate a short url and get the value.
    redirect_object = Redirect(str(sys.argv[1]))
    print(
        await redirect_object.short_url()
    )  # The short_url is only gauraunteed to exist after you've awaited this


if len(sys.argv) > 1 and str(sys.argv[1]) == "start_httpd":
    run_server()
else:
    asyncio.run(main())
