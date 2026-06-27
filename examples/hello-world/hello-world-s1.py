
from chia.base.ChiaFunction import ChiaFunction, get

@ChiaFunction()
def print_hello_world():
    print("Hello World (#1) from a remote call!")

def main():
    get(print_hello_world.chia_remote())

if __name__ == "__main__":
    main()