from flask import (
    Blueprint, request, jsonify
)
from bg.jobs import export_new_raw_data
from db.model import db, Model

bp = Blueprint('models', __name__, url_prefix='/models')

# TODO API auth


@bp.route('export_new_raw_data', methods=['POST'])
def api__export_new_raw_data():
    data = request.get_json()

    model_id = int(data.get('model_id'))
    data_fname = data.get('data_fname')
    output_fname = data.get('output_fname')
    cutoff = float(data.get('cutoff'))

    error = None
    try:
        assert data_fname is not None, f"Missing data_fname"
        assert output_fname is not None, f"Missing output_fname"
        assert 0.0 <= cutoff < 1.0, f"Invalid cutoff={cutoff}"

        model = db.session.query(Model).filter_by(id=model_id).one_or_none()
        export_new_raw_data(model, data_fname, output_fname, cutoff=cutoff)
    except Exception as e:
        error = str(e)

    return jsonify({
        'error': error
    })
