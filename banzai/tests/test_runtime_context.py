from argparse import Namespace

from banzai.context import Context


def test_context_gets_arguments_from_argparse():
    args = Namespace(a=1, b=2, c=5)
    context = Context(args)
    assert context.a == 1
    assert context.b == 2
    assert context.c == 5
