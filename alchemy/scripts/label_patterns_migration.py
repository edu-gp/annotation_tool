"""
Patterns used to be stored on Tasks.
This script migrates all the patterns from binary-classification tasks
to individual labels (by storing them in LabelPatterns).
"""
from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import (
    Database,
    LabelPatterns,
    Task,
    _raw_data_file_path,
    get_or_create,
)
from alchemy.shared.file_adapters import load_jsonl

db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)

if __name__ == "__main__":
    for task in db.session.query(Task).all():
        labels = task.get_labels()
        if len(labels) == 1:
            label = labels[0]
            print(f'task="{task.name}" id={task.id} label="{label}"')

            patterns = task.get_patterns() or []

            patterns_file = task.default_params.get("patterns_file")
            if patterns_file:
                spacy_patterns = load_jsonl(
                    _raw_data_file_path(patterns_file), to_df=False, data_store='local'
                )
                """
                These are usually in the format of:
                [{'label': 'HEALTHCARE', 'pattern': [{'lower': 'health'}]}, ..]
                """
                for pat in spacy_patterns:
                    patterns += [list(x.values())[0] for x in pat["pattern"]]

            print(f"Add {len(patterns)} patterns to label={label}")
            print(patterns)

            pat = get_or_create(db.session, LabelPatterns, label=label)
            patterns += pat.get_positive_patterns() or []
            pat.set_positive_patterns(patterns)
            db.session.add(pat)
            db.session.commit()

            print(f"Now there are {pat.count()} patterns for label={label}")
            print(pat.get_positive_patterns())
        else:
            print(
                f'** Skip task name="{task.name}" id={task.id}, '
                f"it has {len(labels)} labels."
            )
        print("-" * 80)
