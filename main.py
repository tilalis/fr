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
            'cred': 'cred.json',
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

    incident_id = StringField(id=True, presentational=True)
    confirmed = BooleanField(default=False)
    owner = StringField(presentational=True)
    reliability = IntField(default=0, presentational=True)
    confidence = IntField(default=0, presentational=True)
    created = DateField(presentational=True)

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
            'owner': document.owner.replace(' ', '-')
        }


if __name__ == "__main__":
    for i in range(0, 20):
        with Incident().get(i) as incident:
            incident.reliability = 3
            incident.confirmed = not incident.confirmed
