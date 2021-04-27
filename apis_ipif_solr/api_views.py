import json

from pysolaar import Q
from rest_framework.response import Response
from rest_framework.views import APIView

from apis_ipif_solr.indexes import PersonIndex, StatementIndex


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

            ~~~~~~~~~ Statement Params ~~~~~~~~~
            - statementText     person has statement matching StatementText
            - relatesToPerson   person has statement with relatesToPerson URL or Label
            - memberOf          person has statement with memberOf URL or Label
            - role              person has statement with role URL or Label
            - name              person has statement naming the person (string)
            - from              person has statement not before From date
            - to                person has statement not after To date
            - place             person has statement with place URL or Label


        """

        DEFAULT_PAGE_SIZE = 30
        DEFAULT_PAGE_NUMBER = 1

        params = request.GET

        person_result = PersonIndex

        STATEMENT_PARAM_KEYS = [
            "statementText",
            "relatesToPerson",
            "memberOf",
            "role",
            "name",
            "from",
            "to",
            "place",
        ]

        statement_filter_q_object = None
        for statement_param_key in STATEMENT_PARAM_KEYS:
            statement_param_value = params.get(statement_param_key)
            if statement_param_value:
                print(">> ", statement_param_key, statement_param_value)
                if statement_param_key == "to":
                    param_dict = {"to__lte": statement_param_value}
                elif statement_param_key == "from":
                    param_dict = {"from__gte": statement_param_value}

                else:
                    param_dict = {statement_param_key: statement_param_value}

                if not statement_filter_q_object:
                    statement_filter_q_object = Q(**param_dict)
                else:
                    statement_filter_q_object &= Q(**param_dict)

        if statement_filter_q_object:
            print("filtering")
            person_result.filter_by_distinct_child(
                statement_filter_q_object, field_name="ST"
            )

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
        # update: looking at Solr, seems not to be anything in these fields except ID
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

        # Always going to paginate, as the default size is 30 and default page is 1;
        # otherwise, if params are set, use these
        page_number = int(params.get("page", DEFAULT_PAGE_NUMBER))
        person_result = person_result.paginate(
            page_size=int(params.get("size", DEFAULT_PAGE_SIZE)),
            page_number=page_number - 1,  # PySolaar is 0-indexed
        )

        result = {
            "protocol": {
                "size": len(person_result),
                "totalHits": person_result.count(),
                "page": page_number,
            },
            "persons": person_result,
        }

        return Response(result)
