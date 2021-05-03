from functools import reduce
import json

from pysolaar import Q
from pysolaar.pysolaar import PySolaar
from rest_framework.response import Response
from rest_framework.views import APIView

from apis_ipif_solr.indexes import PersonIndex, StatementIndex


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


def apply_statement_params(queryset, params, statements_parent_key="ST"):
    """ Apply statement-specific filters to a given queryset.
    
    Should be used for all Views where filtering by statement is required
    (Person, Factoid, Source) 
    
    
    ~~~~~~~~~ Statement Params ~~~~~~~~~
    ✅ statementType 
    ✅ statementText     person has statement matching StatementText
    ✅ relatesToPerson   person has statement with relatesToPerson URL or Label
    - memberOf          person has statement with memberOf URL or Label
    ✅ role              person has statement with role URL or Label
    ✅ name              person has statement naming the person (string)
    ✅ from              person has statement not before From date
    ✅ to                person has statement not after To date
    ✅ place             person has statement with place URL or Label
    """

    statement_filter_q_list = []
    for statement_param_key in STATEMENT_PARAM_KEYS:
        statement_param_value = params.get(statement_param_key)
        if statement_param_value:
            if statement_param_key == "to":
                statement_filter_q_list.append(
                    Q(date__sortdate__lte=statement_param_value)
                )
            elif statement_param_key == "from":
                statement_filter_q_list.append(
                    Q(date__sortdate__gte=statement_param_value)
                )
            elif statement_param_key in ("statementText", "name", "relatesToPersons"):
                q = Q(**{statement_param_key: statement_param_value})
                statement_filter_q_list.append(q)
            else:
                q = Q(**{f"{statement_param_key}__uri": statement_param_value}) | Q(
                    **{f"{statement_param_key}__label": statement_param_value}
                )
                statement_filter_q_list.append(q)

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


class PersonListView(APIView):
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

        params = request.GET

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

        result = {
            "protocol": {
                "size": len(person_result),
                "totalHits": person_result.count(),
                "page": int(params.get("page", DEFAULT_PAGE_NUMBER)),
            },
            "persons": person_result,
        }

        return Response(result)
