import orm

from orm.document import Document, PresentationDocument
from orm.fields import StringField, IntField, BooleanField, DateField


orm.connect(
    redis={
            'host': '0.0.0.0',
            'port': 1234,
            'db': 0,
            'password': 'password'
    },
    firebase={
            'cred': '',
            'database': 'https://fr.firebaseio.com/',
            'bucket': 'fr.appspot.com',

    }
)


class IncidentPointer(PresentationDocument):
    _container = '/incidentEvents'

    incident_id = StringField(id=True)
    owner = StringField()

    @classmethod
    def presentation(cls, document):
        return {
            'incident_id': document.incident_id,
            'owner': document.owner
        }


class OngoingIncidentPointer(PresentationDocument):
    _container = '/ongoingEvents'

    incident_id = StringField(id=True)
    owner = StringField()

    @classmethod
    def presentation(cls, document):
        return {
            'incident_id': document.incident_id,
            'owner': document.owner
        }


class Incident(Document):
    _container = '/incidents'

    incident_id = StringField(id=True, presentation=True)
    confirmed = BooleanField(default=False)
    action = StringField()
    firstResponder = StringField(presentation=True)
    owner = StringField(presentation=True)
    reliability = IntField(default=0, presentation=True)
    confidence = IntField(default=0, presentation=True)
    created = DateField(presentation=True)

    @staticmethod
    def on_save(document):
        ip = IncidentPointer(
            **document,
            ignore_non_existing=True
        )

        op = OngoingIncidentPointer(
            **document,
            ignore_non_existing=True
        )

        if document.confirmed:
            ip.save(force=True)
            op.delete()
        else:
            op.save(force=True)
            ip.delete()

    @staticmethod
    def on_delete(document):
        IncidentPointer(
            **document,
            ignore_non_existing=True
        ).delete()

        OngoingIncidentPointer(
            **document,
            ignore_non_existing=True
        ).delete()

    @classmethod
    def presentation(cls, document):
        return {
            'id': document.incident_id,
            'rel': document.reliability * document.confidence,
            'created': document.created.isoformat(),
            'owner': document.owner.replace(' ', '-'),
            'firstResponder': document.firstResponder
        }


if __name__ == "__main__":
    # TODO: Add unit tests

    import datetime
    from enum import Enum

    class Type(Enum):
        CREATE = 1
        DELETE = 2
        UPDATE_PRESENTATION = 3
        UPDATE_CACHE = 4
        UPDATE_FULL = 5

    action = Type.UPDATE_PRESENTATION

    for i in range(0, 20):
        if action == Type.CREATE:
            Incident(
                incident_id=i,
                owner='123',
                reliability=i % 3,
                confidence=i * 2,
                confirmed=not bool(i % 2),
                created=datetime.datetime.utcnow()
            ).save()
        elif action == Type.DELETE:
            Incident.get(i).delete()
        elif action == Type.UPDATE_PRESENTATION:
            with Incident.get(i) as incident:
                incident.reliability = i / 4
        elif action == Type.UPDATE_CACHE:
            with Incident.get(i) as incident:
                incident.confirmed = not incident.confirmed
        elif action == Type.UPDATE_FULL:
            with Incident.get(i) as incident:
                incident.firstResponder = 'AAB'
                incident.action = 'cleared'


