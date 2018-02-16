import datetime

import orm
from orm.document import Document, PresentationDocument
from orm.fields import StringField, IntField, BooleanField, DateField


orm.connect(
    redis={
            'host': '0.0.0.0',
            'port': 0,
            'db': 0,
            'password': ''
    },
    firebase={
            'cred': '',
            'database': '',
            'bucket': '',

    }
)


class IncidentPointer(PresentationDocument):
    _container = '/incidentEvents'

    incident_id = StringField(id=True)
    owner = StringField()

    @staticmethod
    def presentation(document):
        return {
            'incident_id': document.incident_id,
            'owner': document.owner
        }


class Incident(Document):
    _container = '/incidents'

    incident_id = StringField(id=True)
    confirmed = BooleanField()
    owner = StringField()
    reliability = IntField()
    confidence = IntField()
    created = DateField()

    @staticmethod
    def on_save(document):
        IncidentPointer(
            **document,
            ignore_non_existing=True
        ).save(force=True)

    @staticmethod
    def presentation(document):
        return {
            'id': document.incident_id,
            'rel': document.reliability * document.confidence,
            'created': document.created.isoformat(),
            'owner': document.owner.replace(' ', '-')
        }


if __name__ == "__main__":
    incident = Incident(incident_id='123')
    incident.reliability = 3
    incident.confidence = 4
    incident.created = datetime.datetime.utcnow()
    incident.owner = '123'
    incident.save()
