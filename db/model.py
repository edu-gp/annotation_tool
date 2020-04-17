from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
metadata = db.Model.metadata


class Label(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True, nullable=False)
    # A label can be part of many annotations.
    annotations = db.relationship('Annotation', backref='label',
                                  lazy='dynamic')
    entity_type_id = db.Column(db.Integer, db.ForeignKey('entity_type.id'))


class EntityType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True, nullable=False)
    # TODO how should we setup the lazy loading mode?
    labels = db.relationship('Label', backref='entity_type', lazy='dynamic')
    entities = db.relationship('Entity', backref='entity_type', lazy='dynamic')

    def __repr__(self):
        return '<EntityType {}>'.format(self.name)


class Entity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True, nullable=False)
    # An entity can have many annotations on it.
    annotations = db.relationship('Annotation', backref='entity',
                                  lazy='dynamic')
    entity_type_id = db.Column(db.Integer, db.ForeignKey('entity_type.id'))

    def __repr__(self):
        return '<Entity {}>'.format(self.name)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True,
                         nullable=False)
    # A user can do many annotations.
    annotations = db.relationship('Annotation', backref='user', lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)


class Context(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    hash = db.Column(db.String(128), index=True, unique=True, nullable=False)
    data = db.Column(db.JSON, nullable=False)
    # A context can be part of many annotations.
    annotations = db.relationship('Annotation', backref='context',
                                  lazy='dynamic')

    def __repr__(self):
        return '<Context {}: {}>'.format(self.id, self.data)


class Annotation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False)
    last_updated_at = db.Column(db.DateTime, nullable=False)

    entity_id = db.Column(db.Integer, db.ForeignKey('entity.id'))
    label_id = db.Column(db.Integer, db.ForeignKey('label.id'))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    context_id = db.Column(db.Integer, db.ForeignKey('context.id'))

    def __repr__(self):
        return """
        Annotation {}:
        Entity Id {}, 
        Label Id {},
        User Id {},
        Context Id {},
        Value {},
        Created at {},
        Last Updated at {}
        """.format(
            self.id,
            self.entity_id,
            self.label_id,
            self.user_id,
            self.context_id,
            self.value,
            self.created_at,
            self.last_updated_at
        )
