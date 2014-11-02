
def map(ctx, key, value):
    ctx.write(1, 2)


def reduce(ctx, key, value):
    ctx.write("hola mundo")

