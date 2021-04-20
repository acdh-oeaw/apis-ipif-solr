from pysolaar import (
    PySolaar,
    SingleValue,
    DocumentFields,
    TransformKey,
    TransformValues,
    JsonToDict,
)
from pysolaar.document import (
    DocumentFields,
    ChildDocument,
    JsonChildDocument,
    SplattedChildDocument,
)
import copy
import hashlib
import re
import json

from django.contrib.contenttypes.models import ContentType
from django.urls import reverse
from apis_core.apis_entities.models import Person
from reversion.models import Version
from apis_bibsonomy.models import Reference
import zlib


PySolaar.configure_pysolr("http://localhost:8983/solr/test_solr", always_commit=True)


STATEMENT_REF_DESCRIPTION = (
    "References the local ID of a statement he current service."
    " Please use the statements/{id}-endpoint to request further"
    " information on the statement and its content"
)


class FactoidIndex(PySolaar):
    class Meta:
        store_document_fields = DocumentFields(
            createdBy=True,
            createdWhen=True,
            modifiedBy=True,
            modifiedWhen=True,
            personId=True,
            ST=SplattedChildDocument(
                id=True,
                name=True,
                memberOf=True,
                place=True,
                relatesToPerson=True,
                role=True,
                statementText=True,
            ),
            Person=ChildDocument(
                id=True,
                uris=True,
            ),
            Statements=ChildDocument(
                id=True,
                statementText=True,
                relatesToPerson=True,
                memberOf=True,
                role=True,
                name=True,
                date=True,
                place=True,
            ),
        )

        return_document_fields = DocumentFields(
            id=TransformKey("@id"),
            createdBy=SingleValue,
            createdWhen=SingleValue,
            modifiedBy=SingleValue,
            modifiedWhen=SingleValue,
            Person=(
                ChildDocument(
                    id=TransformKey("@id"),
                )
                & SingleValue
                & TransformKey("person-ref")
            ),
            Statements=(
                ChildDocument(
                    id=TransformKey("@id"),
                )
                & TransformKey("statement-refs")
                & TransformValues(
                    lambda v: {"description": STATEMENT_REF_DESCRIPTION, **v}
                )
            ),
        )

    def build_document_set(self):
        """The Factoid document set identifiers are all combinations of
        Person x [Person->Sources, None]"""
        for person in Person.objects.all():
            references = Reference.objects.filter(object_id=person.pk)
            for source in [*references, None]:
                print(
                    f"Building FactoidIndex for [Person:{person.pk}, Source:{source}]"
                )
                yield self.build_document((person, source))

    # build_document_set = None

    def build_document(self, identifier):
        """A factoid identifier comprises a tuple of (Person, Source)

        Required search fields:

        - personId    ✅       # personId
        - p           ✅       # Person --   all identifiers of person to whom factoid relates (inc. ID?)
        - statementId ✅       # Statements --  (just use ID) list of all the statement IDs attached to this factoid
        - st          ✅       # st (splat all the ) full-text search over all statements related to factoid (SplattedChildDocument)
        - sourceId          # the source ID to which factoid relates (use SourceIndex to generate, as ID will be some odd combo)
        - s                 # all identifiers (URI, label) of source
        - f                 # factoid metadata

        STATEMENT RELATIONS -- can be combined, so used nested ChildDocument
            - statementText     # Statement Child Object
            - relatesToPerson   # Statement Child Object
            - memberOf          # Statement Child Object
            - role              # Statement Child Object
            - name              # Statement Child Object
            - from              # Statement Child Object # /date
            - to                # Statement Child Object # /date
            - place             # Statement Child Object


        """
        # instance, source = identifier
        person, source = identifier
        res = {}
        ver = Version.objects.get_for_object(person).order_by("revision__date_created")
        if ver:
            res["createdBy"] = str(ver.first().revision.user)
            res["createdWhen"] = ver.first().revision.date_created
            res["modifiedBy"] = str(ver.last().revision.user)
            res["modifiedWhen"] = ver.last().revision.date_created

        res["personId"] = person.pk

        # if "source-ref" not in skip_fields:
        """
        res["source-ref"] = SourceRefGenerator(
            instance,
            source=self._source,
            person=self._person,
            attribute=self._attribute,
        ).to_representation(instance)
        # if "statement-refs" not in skip_fields:
         """

        res["ST"] = StatementIndex.items([(person, source)])
        res["S"] = SourceIndex.items([(person, source)])
        res["Statements"] = StatementIndex.items([(person, source)])
        res["Person"] = PersonIndex.items([person])

        return FactoidIndex.Document(id=f"factoid_{person.pk}", **res)


class PersonIndex(PySolaar):
    class Meta:
        store_document_fields = DocumentFields(
            id=True,
            label=True,
            createdBy=True,
            createdWhen=True,
            modifiedBy=True,
            modifiedWhen=True,
            uris=True,
            S=ChildDocument(
                id=True,
                uris=True,
                label=True,
            ),
            ST=ChildDocument(
                id=True,
                uris=True,
                label=True,
            ),
            F=JsonChildDocument(
                id=True,
                uris=True,
                label=True,
                S=ChildDocument(
                    id=True,
                ),
                Person=ChildDocument(id=True),
                ST=ChildDocument(id=True),
            ),
        )
        return_document_fields = DocumentFields(
            id=TransformKey("@id"),
            label=SingleValue,
            createdBy=SingleValue,
            createdWhen=SingleValue,
            modifiedBy=SingleValue,
            modifiedWhen=SingleValue,
            uris=True,
            F=JsonToDict(
                id=TransformKey("@id"),
                Person=ChildDocument(
                    id=TransformKey("@id"),
                )
                & SingleValue
                & TransformKey("person-ref"),
                ST=ChildDocument(
                    id=TransformKey("@id"),
                )
                & TransformKey("statement-refs"),
                S=ChildDocument(
                    id=TransformKey("@id"),
                )
                & SingleValue
                & TransformKey("source-ref"),
            )
            & TransformKey("factoid-refs"),
        )

    def build_document_set(self):
        for person in Person.objects.all():
            print(f"Building PersonIndex for [Person:{person.pk}]")

            yield self.build_document(person)

    # build_document_set = None

    def build_document(self, instance):
        doc = {"id": instance.pk}
        doc["label"] = f"{instance.name}, {instance.first_name} ({instance.pk})"
        ver = Version.objects.get_for_object(instance).order_by(
            "revision__date_created"
        )
        if ver:
            doc["createdBy"] = str(ver.first().revision.user)
            doc["createdWhen"] = ver.first().revision.date_created
            doc["modifiedBy"] = str(ver.last().revision.user)
            doc["modifiedWhen"] = ver.last().revision.date_created
        doc["uris"] = [
            str(x) for x in instance.uri_set.all().values_list("uri", flat=True)
        ]

        doc["ST"] = StatementIndex.items(
            [
                (instance, source)
                for source in [None, *Reference.objects.filter(object_id=instance.pk)]
            ]
        )

        doc["S"] = SourceIndex.items(
            [
                (instance, source)
                for source in [None, *Reference.objects.filter(object_id=instance.pk)]
            ]
        )
        doc["F"] = FactoidIndex.items(
            [
                (instance, source)
                for source in [None, *Reference.objects.filter(object_id=instance.pk)]
            ]
        )

        return PersonIndex.Document(**doc)


class StatementIndex(PySolaar):
    class Meta:
        store_document_fields = DocumentFields(
            id=True,
            uris=True,
            statementType=True,
            name=True,
            role=True,
            date=True,
            places=True,
            relatesToPersons=ChildDocument(
                id=True,
                uris=True,
            ),
            memberOf=True,
            statementText=True,
            createdBy=True,
            createdWhen=True,
            modifiedBy=True,
            modifiedWhen=True,
            P=ChildDocument(
                id=True,
                uris=True,
                label=True,
                createdBy=True,
                modifiedBy=True,
            ),
            F=ChildDocument(
                id=True,
                uris=True,
                label=True,
                createdBy=True,
                modifiedBy=True,
            ),
            S=ChildDocument(
                id=True,
                uris=True,
                label=True,
                createdBy=True,
                modifiedBy=True,
            ),
        )
        return_document_fields = DocumentFields(
            id=TransformKey("@id"),
            uris=True,
            statementType__label=SingleValue,
            statementType__uri=True,
            name=True,
            role__label=SingleValue,
            role__uri=True,
            date__sortdate=True,
            date__label=True,
            memberOf__label=SingleValue,
            memberOf__uri=True,
            statementText=SingleValue,
            relatesToPersons=ChildDocument(
                uris=True,
                label=SingleValue,
            ),
            createdBy=SingleValue,
            createdWhen=SingleValue,
            modifiedBy=SingleValue,
            modifiedWhen=SingleValue,
        )

    def build_document_set(self):
        """The Statement document set identifiers are all combinations of
        Person x [Person->Sources, None]"""
        for person in Person.objects.all():
            references = Reference.objects.filter(object_id=person.pk)
            for source in [*references, None]:
                print(
                    f"Building StatementIndex for [Person:{person.pk}, Source:{source}]"
                )
                yield from self.build_document((person, source))

    # build_document_set = None

    def build_document(self, identifier):

        instance, _source = identifier

        r_list = []

        # Get relation types for a person, exclude PersonPerson as more complex (tackle below)
        relation_types = ContentType.objects.filter(
            app_label="apis_relations", model__icontains="person"
        ).exclude(model="PersonPerson")

        if _source:
            # If we have a source ID provided, look up that up in references
            references = Reference.objects.filter(bibs_url=_source)

        # Go through each relation type ('PersonInstitute', 'PersonPlace', 'PersonEvent'...)
        # and get the specific relations of each type for the person instance
        for relation_type in relation_types:

            # If we provide a source to limit by, add that as a filter
            if _source:
                relation_queryset = relation_type.model_class().objects.filter(
                    related_person=instance,
                    pk__in=list(references.values_list("object_id", flat=True)),
                )
            # Otherwise get them all...
            else:
                relation_queryset = relation_type.model_class().objects.filter(
                    related_person=instance
                )

            for relation in relation_queryset:

                relation_type_name = relation_type.model_class().__name__

                item = {"@id": f"{instance.pk}_{relation_type_name}_{relation.pk}"}

                item["name"] = ""
                item["statementType"] = {"uri": "", "label": ""}
                item["memberOf"] = {"uri": "", "label": ""}
                item["place"] = {"uri": "", "label": ""}
                # item["relatesToPerson"] = {"uri": "", "label": ""}
                item["role"] = {"url": "NONE", "label": ""}
                item["date"] = {"sortdate": None, "label": ""}

                item["statementType"][
                    "url"
                ] = "statement_type_url"  # relation.relation_type.pk
                item["statementType"][
                    "label"
                ] = "statement_type_label"  # relation.relation_type.label

                item["role"]["label"] = (
                    getattr(relation.relation_type, "reverse_name", None)
                    or relation.relation_type.name
                )
                item["date"]["sortdate"] = relation.start_date or relation.end_date
                item["date"]["label"] = (
                    f"{relation.start_date}-{relation.end_date}"
                    if relation.start_date and relation.end_date
                    else str(relation.start_date)
                )

                if relation_type_name == "PersonInstitution":
                    item["memberOf"]["uri"] = [
                        str(url) for url in relation.related_institution.uri_set.all()
                    ] + [relation.related_institution.get_absolute_url()]
                    item["memberOf"]["label"] = relation.related_institution.name

                if relation_type_name == "PersonPlace":
                    item["place"]["uri"] = [
                        str(url) for url in relation.related_place.uri_set.all()
                    ] + [relation.related_place.get_absolute_url()]
                    item["place"]["label"] = relation.related_place.name

                item["statementText"] = str(relation)

                """
                if (
                    not skip_factoid_refs
                ):  # TODO: these are not generating factoid refs!!!
                    references = Reference.objects.filter(object_id=instance.pk)
                    item["factoid-refs"] = [
                        factoidRef
                        for factoidRef in [
                            FactoidRefGenerator(
                                instance, source=getattr(ref, "bibs_url", None)
                            ).to_representation(instance, skip_fields="person-ref")
                            for ref in [*references, None]
                        ]
                        if item["@id"]
                        in {s["@id"] for s in factoidRef["statement-refs"]}
                    ]
                """
                ver = Version.objects.get_for_object(relation).order_by(
                    "revision__date_created"
                )
                if ver:
                    item["createdBy"] = str(ver.first().revision.user)
                    item["createdWhen"] = ver.first().revision.date_created
                    item["modifiedBy"] = str(ver.last().revision.user)
                    item["modifiedWhen"] = ver.last().revision.date_created

                r_list.append(item)

        # Here we do same as above, for PersonPerson relations.
        # But obviously more complex as persons can be A or B in a relation

        # Build specific relation sets, and assign whether instance is A or B in relation
        for A_or_B, relation_set in [
            *[
                ("A", rel)
                for rel in [
                    relation_type_set.personperson_set.filter(
                        related_personA=instance
                    ).all()
                    for relation_type_set in instance.personB_relationtype_set.all()
                ]
            ],
            *[
                ("B", rel)
                for rel in [
                    relation_type_set.personperson_set.filter(
                        related_personB=instance
                    ).all()
                    for relation_type_set in instance.personA_relationtype_set.all()
                ]
            ],
        ]:

            # Iterate all the relations in each set
            for relation in relation_set:

                item = {
                    "@id": f"{instance.pk}__PersonPerson_{relation.relation_type.pk}__{relation.pk}"
                }

                item["name"] = ""
                item["statementType"] = {"uri": "", "label": ""}
                item["memberOf"] = {"uri": "", "label": ""}
                item["place"] = {"uri": "", "label": ""}
                item["relatesToPerson"] = {"uri": "", "label": ""}
                item["role"] = {"uri": "NONE", "label": ""}
                item["date"] = {"sortdate": None, "label": ""}

                # Unpack related person, depending on role A or B
                item["relatesToPersons"] = (
                    PersonIndex.items(relation.related_personB)
                    if A_or_B == "A"
                    else PersonIndex.items(relation.related_personA)
                )
                """
                item["relatesToPerson"] = (
                    {
                        "uri": [
                            str(uri) for uri in relation.related_personB.uri_set.all()
                        ],
                        "label": f"{relation.related_personB.name}, {relation.related_personB.first_name}",
                    }
                    if A_or_B == "A"
                    else {
                        "uri": [
                            str(uri) for uri in relation.related_personA.uri_set.all()
                        ],
                        "label": f"{relation.related_personA.name}, {relation.related_personA.first_name}",
                    }
                )
                """

                item["statementType"][
                    "uri"
                ] = "statement_type_uri"  # relation.relation_type.pk
                item["statementType"][
                    "label"
                ] = "statement_type_label"  # relation.relation_type.label

                item["role"]["label"] = (
                    relation.relation_type.name
                    if A_or_B == "A"
                    else relation.relation_type.name_reverse
                )
                item["date"]["sortdate"] = relation.start_date or relation.end_date
                item["date"]["label"] = (
                    f"{relation.start_date}-{relation.end_date}"
                    if relation.start_date and relation.end_date
                    else relation.start_date_written
                    if relation.start_date_written
                    else str(relation.start_date)
                )

                item["statement-text"] = str(relation)

                ver = Version.objects.get_for_object(relation).order_by(
                    "revision__date_created"
                )
                item["createdBy"] = str(ver.first().revision.user)
                item["createdWhen"] = ver.first().revision.date_created
                item["modifiedBy"] = str(ver.last().revision.user)
                item["modifiedWhen"] = ver.last().revision.date_created

                """
                if not skip_factoid_refs:
                    references = Reference.objects.filter(object_id=instance.pk)
                    item["factoid-refs"] = [
                        factoidRef
                        for factoidRef in [
                            FactoidRefGenerator(
                                instance, source=getattr(ref, "bibs_url", None)
                            ).to_representation(instance, skip_fields="person-ref")
                            for ref in [*references, None]
                        ]
                        if item["@id"]
                        in {s["@id"] for s in factoidRef["statement-refs"]}
                    ]
                """
                r_list.append(item)

        # Iterate over _meta.fields and then get the value
        # calling .value_to_string on object for each _meta.field
        for f in instance._meta.fields:
            """
            From this loop it seems we take:
            - name -- apis_metainfo.TempEntityClass.name
            - date-of-birth -- apis_metainfo.TempEntityClass.start_date
            - date-of-death -- apis_metainfo.TempEntityClass.end_date
            - first-name -- apis_entities.Person.first_name
            - gender -- apis_entities.Person.gender

            """
            field_name = re.search(r"([A-Za-z]+)\'>", str(f.__class__)).group(1)
            if (
                field_name
                in [
                    "CharField",
                    "DateField",
                    "DateTimeField",
                    "IntegerField",
                    "FloatField",
                    "ForeignKey",
                ]
                and f.name.lower() not in {"source", "status"}
            ):
                if _source and f.name in list(
                    references.values_list("attribute", flat=True)
                ):
                    item = {"@id": f"{instance.pk}_attrb_{f.name}"}

                else:
                    item = {"@id": f"{instance.pk}_attrb_{f.name}"}

                item["statementText"] = ""

                item["name"] = ""
                item["statementType"] = {"uri": "", "label": ""}
                item["memberOf"] = {"uri": "", "label": ""}
                item["place"] = {"uri": "", "label": ""}
                # item["relatesToPerson"] = {"uri": "", "label": ""}
                item["role"] = {"uri": "NONE", "label": ""}
                item["date"] = {"sortdate": None, "label": ""}

                if f.name in {"name", "first_name"}:
                    item["statementType"]["uri"] = "statement_type_uri"
                    item["role"]["label"] = f"has {f.name.replace('_', ' ')}"
                    item["name"] = f.value_to_string(instance)

                if f.name == "gender":
                    item["statementType"]["uri"] = "statement_type_uri"
                    item["statementType"]["label"] = f.value_to_string(instance)
                    item["role"]["label"] = "gender"
                    item["statementText"] = f"has gender {f.value_to_string(instance)}"

                if f.name == "start_date":
                    item["statementType"]["uri"] = "statement_type_uri"
                    item["role"]["label"] = "birth"
                    item["date"]["sortdate"] = f.value_from_object(instance)

                if f.name == "end_date":
                    item["statementType"]["uri"] = "statement_type_uri"
                    item["role"]["label"] = "death"
                    item["date"]["sortdate"] = f.value_from_object(instance)

                """
                if not skip_factoid_refs:
                    references = Reference.objects.filter(object_id=instance.pk)
                    item["factoid-refs"] = [
                        factoidRef
                        for factoidRef in [
                            FactoidRefGenerator(
                                instance, source=getattr(ref, "bibs_url", None)
                            ).to_representation(instance)
                            for ref in [*references, None]
                        ]
                        if item["@id"]
                        in {s["@id"] for s in factoidRef["statement-refs"]}
                    ]
                """
                if item["role"]["label"]:
                    r_list.append(item)

        # Iterate many to many-to-many fields
        for field_set in instance._meta.many_to_many:
            if str(field_set.related_model.__module__).endswith(
                "apis_vocabularies.models"
            ) and not field_set.name.endswith("set"):

                # If we limit by source, skip the field
                if _source:
                    if field_set.name not in list(
                        references.values_list("attribute", flat=True)
                    ):
                        continue

                # Meta fields have more than one value (potentially), so iterate...
                for field in getattr(instance, field_set.name).all():
                    item = {"@id": f"{instance.pk}_m2m_{field_set.name}_{field.pk}"}
                    item["statementType"] = {"uri": "", "label": field.name}
                    item["statementText"] = ""

                    item["name"] = ""
                    item["statementType"] = {"uri": "", "label": ""}
                    item["memberOf"] = {"uri": [], "label": ""}
                    item["place"] = {"uri": "", "label": ""}
                    # item["relatesToPerson"] = {"uri": "", "label": ""}

                    item["role"] = {"uri": "NONE", "label": field_set.name}
                    item["date"] = {"sortdate": None, "label": ""}
                    """
                    if not skip_factoid_refs:
                        references = Reference.objects.filter(object_id=instance.pk)
                        item["factoid-refs"] = [
                            factoidRef
                            for factoidRef in [
                                FactoidRefGenerator(
                                    instance, source=getattr(ref, "bibs_url", None)
                                ).to_representation(instance, skip_fields="person-ref")
                                for ref in [*references, None]
                            ]
                            if item["@id"]
                            in {s["@id"] for s in factoidRef["statement-refs"]}
                        ]
                    """
                    r_list.append(item)
        for item in r_list:
            # print(item)
            item["P"] = PersonIndex.items([instance])
            item["F"] = FactoidIndex.items([(instance, _source)])
            item["S"] = SourceIndex.items([(instance, _source)])
        yield from (
            StatementIndex.Document(id=item.pop("@id"), **item) for item in r_list
        )


class SourceIndex(PySolaar):
    class Meta:
        store_document_fields = DocumentFields(
            createdBy=True,
            createdWhen=True,
            modifiedBy=True,
            modifiedWhen=True,
            label=True,
            uris=True,
            st=SplattedChildDocument(
                id=True,
                name=True,
                memberOf=True,
                place=True,
                relatesToPerson=True,
                role=True,
                statementText=True,
            ),
            F=SplattedChildDocument(
                createdBy=True,
                modifiedBy=True,
                modfiedWhen=True,
                createdWhen=True,
            ),
            Factoids=ChildDocument(
                id=True,
                Person=SplattedChildDocument(
                    id=True,
                ),
                Source=SplattedChildDocument(
                    id=True,
                ),
                Statements=SplattedChildDocument(id=True),
            ),
            P=SplattedChildDocument(
                uris=True,
                createdBy=True,
                modifiedBy=True,
            ),
            Persons=ChildDocument(
                id=True,
            ),
            Statements=ChildDocument(
                id=True,
                statementText=True,
                relatesToPerson=True,
                memberOf=True,
                role=True,
                name=True,
                date=True,
                place=True,
            ),
        )
        return_document_fields = DocumentFields(
            id=TransformKey("@id"),
            label=SingleValue,
            uris=True,
            createdBy=SingleValue,
            createdWhen=SingleValue,
            modfiedBy=SingleValue,
            modifiedWhen=SingleValue,
            Factoids=ChildDocument(
                Person=TransformKey("person-ref") & SingleValue,
            )
            & SingleValue
            & TransformKey("factoid-refs"),
        )

    def build_document_set(self):
        """Iterate all persons; for each person, build_document returns iterable of sources

        TODO: NO NO NO: all the Sources are all the Reference objects and (Person,None) combinations.
        If we do it as currently (i.e. from person) then we risk duplicating sources ...

        """
        for person in Person.objects.all():
            references = Reference.objects.filter(object_id=person.pk)
            for source in [*references, None]:
                print(f"Building SourceIndex for [Person:{person.pk}, Source:{source}]")
                yield self.build_document((person, source))

    # build_document_set = None

    def build_document(self, identifier):
        person, source = identifier

        doc = {}

        # All APIS Persons have a None source, in addition to actual sources
        # 'None' is for non-directly-attributed data created in APIS
        if source is None:
            if person.source_id:
                doc["id"] = f"original_source_{person.source_id}"
            else:
                doc["id"] = f"original_source_for_{person.pk}"

            doc["label"] = str(person.source)  # TODO: this is a rubbish label!
            doc["uris"] = [
                reverse("apis:apis_api:source-detail", kwargs={"pk": person.source_id})
            ]
        else:
            doc = {
                "id": f"reference_{person.pk}_{hashlib.md5(str(source.bibs_url).encode('utf-8')).hexdigest()}"
            }
            bib = json.loads(source.bibtex)
            doc["label"] = f"{bib['title']} ({getattr(bib, 'author', '-')})"
            doc["uris"] = [source.bibs_url]
        ver = Version.objects.get_for_object(person).order_by("revision__date_created")
        if ver:
            doc["createdBy"] = str(ver.first().revision.user)
            doc["createdWhen"] = str(ver.first().revision.date_created)
            doc["modifiedBy"] = str(ver.last().revision.user)
            doc["modifiedWhen"] = str(ver.last().revision.date_created)

        if source is None:
            # If Source is None, get the Person-NoneSource combo -- by definition only one item
            doc["Factoids"] = FactoidIndex.items([(person, source)])
            doc["f"] = FactoidIndex.items([(person, source)])
            doc["Persons"] = PersonIndex.items(person)
            doc["p"] = PersonIndex.items(person)
            doc["Statements"] = StatementIndex.items([(person, source)])
            doc["st"] = StatementIndex.items([(person, source)])
        else:
            # Otherwise, there can be more than one Factoid related to this source (i.e. same source
            # but more than one person, so iterate persons if they are connected to this reference)
            persons_associated_with_source = [
                p
                for p in Person.objects.all()
                if source in Reference.objects.filter(object_id=p.pk)
            ]

            doc["Factoids"] = FactoidIndex.items(
                [(p, source) for p in persons_associated_with_source]
            )
            doc["f"] = FactoidIndex.items(
                [(p, source) for p in persons_associated_with_source]
            )

            doc["Persons"] = PersonIndex.items(persons_associated_with_source)
            doc["p"] = PersonIndex.items(persons_associated_with_source)
            doc["Statements"] = StatementIndex.items(
                [(p, source) for p in persons_associated_with_source]
            )

            doc["st"] = StatementIndex.items(
                [(p, source) for p in persons_associated_with_source]
            )

        return self.Document(**doc)


def run():
    # p = Person.objects.first()
    # d = FactoidIndex.build_document((p, None)).doc_to_solr()
    # print(len(d["_doc"]))

    import time

    start_time = time.time()
    PySolaar.update(max_chunk_size=2000)

    print(PersonIndex.first())
    print(time.time() - start_time)

    """
    d = SourceIndex.build_document(
        (
            Person.objects.filter(pk=29839).first(),
            Reference.objects.filter(object_id=29839).first(),
        )
    ).doc_to_solr()
    print(d)
    """