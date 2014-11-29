


def map(mctx, key, value):
    mctx.write(1, 2)


def reduce(rctx, key, value):
    rctx.write("hola mundo")



