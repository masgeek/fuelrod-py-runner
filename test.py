import typer
from os import getenv


def main(name: str):

    print(f"Hello {name}")


if __name__ == "__main__":
    typer.run(main)
