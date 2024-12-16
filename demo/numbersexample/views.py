from dask import delayed
from rest_framework import viewsets
from rest_framework.request import Request
from rest_framework.decorators import action
from rest_framework.response import Response
from django.http import HttpRequest
from daskmanager.daskmanager import DaskManager
from daskmanager.serializers import DaskTaskSerializer
from numbersexample.fibonacci import fib
from numbersexample.models import Number
from numbersexample.serializers import NumberSerializer
from rest_framework.decorators import api_view, schema


class NumberViewSet(viewsets.ModelViewSet):
    serializer_class = NumberSerializer
    queryset = Number.objects.all()

    @action(['get'], detail=False)
    def square_sum(self, request, pk=None):
        # Get objects from database
        numbers = list(Number.objects.all().values_list('value', flat=True))

        # Build graph
        squares = []
        for number in numbers:
            number_squared = delayed(lambda n: n**2)(number)
            squares.append(number_squared)
        sum_squares = delayed(sum)(squares)

        # Submit graph to Dask
        dask_task = DaskManager().compute(sum_squares)

        # Return task
        return Response(
            DaskTaskSerializer(
                instance=dask_task, context=self.get_serializer_context()
            ).data
        )

    @action(methods=['post', 'get'], detail=False)
    def do_fibonacci(self, request: Request):
        if request.method == 'POST':
            some_value = request.POST.get('value', 1)
            if isinstance(some_value, str):
                some_value = int(some_value)

            fib_result = delayed(fib)(some_value)

            # Submit graph to Dask
            dask_task = DaskManager().compute(fib_result)

            # Return task
            return Response(
                DaskTaskSerializer(
                    instance=dask_task, context=self.get_serializer_context()
                ).data
            )
        elif request.method == 'GET':
            return Response({'message': 'Send 'some_value''})
        else:
            raise ValueError('Unsupported HTTP method')


@schema(None)
@api_view(['GET', 'POST'])
def hello_world(request):
    if request.method == 'POST':
        return Response({'message': 'Got some data!', 'data': request.data})
    return Response({'message': 'Hello, world!'})


@schema(None)
@api_view(['GET', 'POST'])
def fibonacci(request):
    if request.method == 'POST':
        return Response({'message': 'Got some data!', 'data': request.data})
    return Response({'message': 'Hello, world!'})
