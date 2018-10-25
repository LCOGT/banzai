from banzai.context import PipelineContext
from argparse import Namespace


def test_pipeline_context_gets_arguments_from_argparse():
    args = Namespace(a=1, b=2, c=5)
    pipeline_context = PipelineContext(args, [])
    assert pipeline_context.a == 1
    assert pipeline_context.b == 2
    assert pipeline_context.c == 5


def test_pipeline_context_gets_arguments_from_kwargs():
    kwargs = {'a': 6, 'b': 7, 'c': 11}
    pipeline_context = PipelineContext(Namespace(), [], **kwargs)
    assert pipeline_context.a == 6
    assert pipeline_context.b == 7
    assert pipeline_context.c == 11
