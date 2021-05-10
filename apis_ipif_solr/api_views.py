import datetime
from functools import reduce

from dateutil.parser import parse
from pysolaar import Q, PySolaar
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound

from apis_ipif_solr.indexes import (
    FactoidIndex,
    PersonIndex,
    SourceIndex,
    StatementIndex,
)


DEFAULT_PAGE_SIZE = 30
DEFAULT_PAGE_NUMBER = 1
STATEMENT_PARAM_KEYS = [
    "statementType",
    "statementText",
    "relatesToPersons",
    "memberOf",
    "role",
    "name",
    "from",
    "to",
    "place",
]


def apply_page_number_and_size_params(queryset, params):
    # Always going to paginate, as the default size is 30 and default page is 1;
    # otherwise, if params are set, use these
    page_number = int(params.get("page", DEFAULT_PAGE_NUMBER))
    queryset = queryset.paginate(
        page_size=int(params.get("size", DEFAULT_PAGE_SIZE)),
        page_number=page_number - 1,  # PySolaar is 0-indexed
    )
    return queryset


def build_statement_filter_q_list(params):
    """ Unpacks Statement-related parameters into a list of Q objects
    depending on parameter type"""

    statement_filter_q_list = []
    for statement_param_key in STATEMENT_PARAM_KEYS:
        statement_param_value = params.get(statement_param_key)
        if statement_param_value:
            if statement_param_key == "to":
                statement_filter_q_list.append(
                    Q(
                        # date__sortdate_dt="[* TO *]",
                        date__sortdate_dt__lte=parse(
                            statement_param_value,
                            default=datetime.datetime(2022, 12, 31),
                        ),
                    )
                )
            elif statement_param_key == "from":
                statement_filter_q_list.append(
                    Q(
                        # date__sortdate_dt="[* TO *]",
                        date__sortdate_dt__gte=parse(
                            statement_param_value, default=datetime.datetime(1000, 1, 1)
                        ),
                    )
                )
            elif statement_param_key == "place":
                # Fields are called "places" but API wants "place"
                q = Q(places__uris=statement_param_value) | Q(
                    places__label=statement_param_value
                )
                statement_filter_q_list.append(q)
            elif statement_param_key in ("statementText", "name", "relatesToPersons"):
                q = Q(**{statement_param_key: statement_param_value})
                statement_filter_q_list.append(q)
            else:
                q = Q(**{f"{statement_param_key}__uri": statement_param_value}) | Q(
                    **{f"{statement_param_key}__label": statement_param_value}
                )
                statement_filter_q_list.append(q)

    return statement_filter_q_list


def apply_statement_params(queryset, params, statements_parent_key="ST"):
    """ Apply statement-specific filters to a given queryset.
    
    Should be used for all Views where filtering by statement is required
    (Person, Factoid, Source) 
    
    
    ~~~~~~~~~ Statement Params ~~~~~~~~~
    ✅ statementType 
    ✅ statementText     person has statement matching StatementText
    ✅ relatesToPerson   person has statement with relatesToPerson URL or Label
    ✅ memberOf          person has statement with memberOf URL or Label
    ✅ role              person has statement with role URL or Label
    ✅ name              person has statement naming the person (string)
    ✅ from              person has statement not before From date
    ✅ to                person has statement not after To date
    ✅ place             person has statement with place URL or Label
    """

    statement_filter_q_list = build_statement_filter_q_list(params)

    if statement_filter_q_list:
        #######
        # All the statement-params should apply to a single statement
        #######
        if params.get("combineStatementFilters") == "and":
            statement_filter_q_object = statement_filter_q_list[0]
            for q in statement_filter_q_list[1:]:
                statement_filter_q_object &= q
            queryset = queryset.filter_by_distinct_child(
                statement_filter_q_object, field_name=statements_parent_key
            )
        #######
        # At least one of the statement-params needs to apply to any statement
        #######
        elif params.get("combineStatementFilters") == "or":
            statement_filter_q_object = statement_filter_q_list[0]
            for q in statement_filter_q_list[1:]:
                statement_filter_q_object |= q
            queryset = queryset.filter_by_distinct_child(
                statement_filter_q_object, field_name=statements_parent_key
            )
        #######
        # Each statement-param must apply, but can be to distinct statements
        #######
        else:  # params["combineStatementFilters"] == "independent"
            for q in statement_filter_q_list:
                queryset = queryset.filter_by_distinct_child(
                    q, field_name=statements_parent_key
                )

    return queryset


def wrap_result_with_protocol(result, params, ipif_type):
    return {
        "protocol": {
            "size": len(result),
            "totalHits": result.count(),
            "page": int(params.get("page", DEFAULT_PAGE_NUMBER)),
        },
        ipif_type: result,
    }


class PersonsListView(APIView):
    def get(self, request, format=None):
        """
        Get list of persons from index.

        Params:

            ~~~~~~~~~ Generic Params ~~~~~~~~~
            ✅ size
            ✅ page
            - sortBy # NOT IMPLEMENTED IN PYSOLAAR 
            ✅ p                 person metadata full search
            ✅ factoidId         has factoid with Id
            ✅ f                 factoid metadata full search
            ✅ statementId       has statement with Id
            ✅ st                statement (meta)data full search
            ✅ sourceId          has source with Id
            ✅ s                 source metadata full search
                

            


        """

        params = request.query_params

        person_result = PersonIndex

        person_result = apply_statement_params(person_result, params)
        person_result = apply_page_number_and_size_params(person_result, params)

        p = params.get("p")
        if p:
            person_result = person_result.filter(
                Q(label=p) | Q(createdBy=p) | Q(modifiedBy=p) | Q(uris=p)
            )

        factoidId = params.get("factoidId")
        if factoidId:
            person_result = person_result.filter_by_distinct_child(
                field_name="F", id=factoidId
            )

        f = params.get("f")
        if f:
            person_result = person_result.filter_by_distinct_child(
                Q(createdBy=f) | Q(modifiedBy=f), field_name="F",
            )

        statementId = params.get("statementId")
        if statementId:
            person_result = person_result.filter_by_distinct_child(
                field_name="ST", id=statementId
            )

        # TODO: not working
        # UPDATE: looking at Solr, seems not to be anything in these fields except ID
        # (which works: see statementId above)
        st = params.get("st")
        if st:
            person_result = person_result.filter_by_distinct_child(
                Q(createdBy=st)
                | Q(modifiedBy=st)
                | Q(statementType__label=st)
                | Q(statementType__uri=st),
                field_name="ST",
            )

        sourceId = params.get("sourceId")
        if sourceId:
            person_result = person_result.filter_by_distinct_child(
                field_name="S", id=sourceId
            )

        s = params.get("s")
        if s:
            person_result = person_result.filter_by_distinct_child(
                Q(label=s) | Q(uris=s) | Q(createdBy=s) | Q(modifiedBy=s),
                field_name="S",
            )

        import datetime

        result = wrap_result_with_protocol(person_result, params, "persons")

        return Response(result)


class PersonsView(APIView):
    def get(self, request, format=None, id=None):

        person_result = PersonIndex.filter(Q(id=id) | Q(uris=id))

        if person_result.first():
            return Response(person_result.first())

        return Response({"description": "the person does not exist"}, status=404)


class FactoidsListView(APIView):
    def get(self, request, format=None):
        params = request.query_params

        factoid_result = FactoidIndex
        factoid_result = apply_page_number_and_size_params(factoid_result, params)
        factoid_result = apply_statement_params(
            factoid_result, params, statements_parent_key="Statements"
        )

        statementId = params.get("statementId")
        if statementId:
            factoid_result = factoid_result.filter_by_distinct_child(
                field_name="ST", id=statementId
            )

        st = params.get("st")
        if st:
            factoid_result = factoid_result.filter_by_distinct_child(
                Q(createdBy=st)
                | Q(modifiedBy=st)
                | Q(statementType__label=st)
                | Q(statementType__uri=st),
                field_name="ST",
            )

        sourceId = params.get("sourceId")
        if sourceId:
            factoid_result = factoid_result.filter_by_distinct_child(
                field_name="S", id=sourceId
            )

        s = params.get("s")
        if s:
            factoid_result = factoid_result.filter_by_distinct_child(
                Q(label=s) | Q(uris=s) | Q(createdBy=s) | Q(modifiedBy=s),
                field_name="S",
            )

        personId = params.get("personId")
        if personId:
            factoid_result = factoid_result.filter_by_distinct_child(
                field_name="Person", id=personId
            )

        p = params.get("p")
        if p:
            factoid_result = factoid_result.filter_by_distinct_child(
                Q(label=p) | Q(uris=p) | Q(createdBy=p) | Q(modifiedBy=p),
                field_name="Person",
            )

        f = params.get("f")
        if f:
            factoid_result = factoid_result.filter_by_distinct_child(
                Q(createdBy=f) | Q(modifiedBy=f), field_name="F",
            )

        result = wrap_result_with_protocol(factoid_result, params, "factoids")
        return Response(result)


class FactoidsView(APIView):
    def get(self, request, format=None, id=None):
        factoid = FactoidIndex.filter(id=id)
        if factoid.first():
            return Response(factoid.first())
        return Response({"description": "the factoid does not exist"}, status=404)


class StatementsListView(APIView):
    def get(self, request, format=None):
        params = request.query_params

        statement_result = StatementIndex

        sourceId = params.get("sourceId")
        if sourceId:
            statement_result = statement_result.filter_by_distinct_child(
                field_name="S", id=sourceId
            )

        s = params.get("s")
        if s:
            statement_result = statement_result.filter_by_distinct_child(
                Q(label=s) | Q(uris=s) | Q(createdBy=s) | Q(modifiedBy=s),
                field_name="S",
            )

        personId = params.get("personId")
        if personId:
            statement_result = statement_result.filter_by_distinct_child(
                field_name="P", id=personId
            )

        p = params.get("p")
        if p:
            statement_result = statement_result.filter_by_distinct_child(
                Q(label=p) | Q(uris=p) | Q(createdBy=p) | Q(modifiedBy=p),
                field_name="P",
            )

        factoidId = params.get("factoidId")
        if factoidId:
            statement_result = statement_result.filter_by_distinct_child(
                field_name="F", id=factoidId
            )

        f = params.get("f")
        if f:
            statement_result = statement_result.filter_by_distinct_child(
                Q(createdBy=f) | Q(modifiedBy=f), field_name="F",
            )

        statement_filter_q_list = build_statement_filter_q_list(params)
        if statement_filter_q_list:
            statement_filter_q_object = statement_filter_q_list[0]
            for q in statement_filter_q_list[1:]:
                statement_filter_q_object &= q

            statement_result = statement_result.filter(statement_filter_q_object)
        statement_result = apply_page_number_and_size_params(statement_result, params)
        result = wrap_result_with_protocol(statement_result, params, "statements")
        return Response(result)


class StatementsView(APIView):
    def get(self, request, format=None, id=None):
        statement = StatementIndex.filter(id=id)
        if statement.first():
            return Response(statement.first())
        return Response({"description": "the statement does not exist"})


class SourcesListView(APIView):
    def get(self, request, format=None):
        params = request.query_params

        source_result = SourceIndex

        s = params.get("s")
        if s:
            source_result = source_result.filter(
                Q(label=s) | Q(uris=s) | Q(createdBy=s) | Q(modifiedBy=s),
            )

        personId = params.get("personId")
        if personId:
            source_result = source_result.filter_by_distinct_child(
                field_name="P", id=personId
            )

        p = params.get("p")
        if p:
            source_result = source_result.filter_by_distinct_child(
                Q(label=p) | Q(uris=p) | Q(createdBy=p) | Q(modifiedBy=p),
                field_name="P",
            )

        factoidId = params.get("factoidId")
        if factoidId:
            source_result = source_result.filter_by_distinct_child(
                field_name="Factoids", id=factoidId
            )

        f = params.get("f")
        if f:
            source_result = source_result.filter_by_distinct_child(
                Q(createdBy=f) | Q(modifiedBy=f), field_name="Factoids",
            )

        source_result = apply_statement_params(source_result, params)
        source_result = apply_page_number_and_size_params(source_result, params)
        result = wrap_result_with_protocol(source_result, params, "sources")
        return Response(result)


class SourcesView(APIView):
    def get(self, request, id, format=None):
        source = SourceIndex.filter(id=id)
        if source.first():
            return Response(source.first())
        return Response({"description": "the statement does not exist"})
