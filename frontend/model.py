from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
migrate = Migrate(db=db)


class Label(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)
    entity_type_id = db.Column(db.Integer, db.ForeignKey('entity_type.id'))


class EntityType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)
    # TODO how should we setup the lazy loading mode?
    labels = db.relationship('Label', backref='entity_type', lazy='dynamic')
    entities = db.relationship('Entity', backref='entity_type', lazy='dynamic')

    def __repr__(self):
        return '<EntityType {}>'.format(self.name)


class Entity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)
    entity_type_id = db.Column(db.Integer, db.ForeignKey('entity_type.id'))

    def __repr__(self):
        return '<Entity {}>'.format(self.name)
