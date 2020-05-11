const e = React.createElement;

class AnnotationBox extends React.Component {
    constructor(props) {
        super(props);

        this.state = this.props['anno'];
    }

    getAnnotationGuide(label) {
        let annotation_guides = this.props['annotation_guides'] || {};
        let guide = annotation_guides[label] || { 'text': 'N/A' };
        return guide['text'];
    }

    isBinaryClassification() {
        return this.props['suggested_labels'].length == 1;
    }

    setLabel(label, value) {
        // console.log("setLabel", label, value)
        var newState = this.state;
        if (newState['labels'] === undefined) {
            newState['labels'] = {};
        }
        newState['labels'][label] = value;
        // console.log(newState);
        this.setState(newState);

        if (this.isBinaryClassification()) {
            // In binary classification, once the user clicks on a button, we submit the results.
            this.submitResults();
        }
    }

    submitResults() {
        var _testing = this.props.testing;

        var dataToSend = JSON.parse(JSON.stringify(this.props));
        dataToSend['anno'] = this.state;

        if (_testing) {
            console.log('submitting...', dataToSend);
        }

        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/tasks/receive_annotation')
        xhr.setRequestHeader("Content-Type", "application/json;charset=UTF-8");
        xhr.addEventListener('load', () => {
            if (_testing) {
                console.log('received', xhr.responseText);
            }

            var resp = JSON.parse(xhr.responseText);
            if (resp.redirect !== undefined) {
                if (_testing) {
                    console.log('redirect to', resp.redirect);
                } else {
                    window.location.href = resp.redirect;
                }
            }
        })
        if (!_testing) {
            xhr.send(JSON.stringify(dataToSend))
        }
    }

    render() {
        let POS_LABEL = 1;
        let NOTSURE_LABEL = 0;
        let NEG_LABEL = -1;

        let req = this.props['req'];
        let suggested_labels = this.props['suggested_labels'];

        let anno = this.state;


        // ------------------------ Content ------------------------

        var content = [];

        // Special Meta Key "domain"
        if (req.data.meta.domain !== undefined) {
            content.push(
                <div key='rendered_domain' style={{ textAlign: "center", padding: '5px' }}>
                    <a href={'http://' + req.data.meta.domain} target="_blank">
                        {req.data.meta.domain}
                    </a>
                </div >
            )
        }

        // Special Meta Key "image_url"
        if (req.data.meta.image_url !== undefined) {
            content.push(
                <div key='rendered_image' style={{ textAlign: "center", padding: '5px' }}>
                    <a href={req.data.meta.image_url} target="_blank">
                        <img src={req.data.meta.image_url} style={{ maxHeight: "300px", maxWidth: "300px" }} />
                    </a>
                </div>
            )
        }

        var textStyle = { padding: '5px' };

        if (req.pattern_info !== undefined && req.pattern_info !== null) {
            let tokens = req.pattern_info.tokens;
            let matches = req.pattern_info.matches;

            var rendered_tokens = [];

            function isMatch(i) {
                /** Easy way to check matches. */
                for (let idx in matches) {
                    let match = matches[idx];
                    if (match[0] <= i && i < match[1]) {
                        return true;
                    }
                }
                return false;
            }

            for (var i = 0; i < tokens.length; i++) {
                if (isMatch(i)) {
                    // Current token was matched
                    rendered_tokens.push(
                        <span style={{ padding: '3px', display: 'inline-block', backgroundColor: "yellow", color: "black" }} key={i.toString()}>{tokens[i]}</span>
                    );
                } else {
                    rendered_tokens.push(
                        <span style={{ padding: '3px', display: 'inline-block' }} key={i.toString()}>{tokens[i]}</span>
                    )
                }
            }

            content.push(
                <div style={textStyle} key='rendered_tokens'>
                    {rendered_tokens}
                </div>
            );
        } else {
            content.push(
                <div style={textStyle} key='rendered_tokens'>
                    {req.data.text}
                </div>
            );
        }

        var meta = []

        let _meta = req.data.meta;
        for (var k in _meta) {
            if (_meta.hasOwnProperty(k)) {
                meta.push(
                    <div key={k}>
                        <span>{k}</span>: <span>{_meta[k]}</span>
                    </div>
                );
            }
        }



        // ------------------------ Controls ------------------------

        var controls = []

        var self = this;

        suggested_labels.forEach(function (label, index) {

            var yes_btn_class = 'btn btn-success';
            var no_btn_class = 'btn btn-danger';
            var skip_btn_class = 'btn btn-secondary';

            var yes_btn_icon = String.fromCharCode(10003); // check
            var no_btn_icon = String.fromCharCode(10007); // cross

            if (anno['labels'][label] !== undefined) {
                if (anno['labels'][label] === POS_LABEL) {
                    yes_btn_class = 'btn btn-success';
                    no_btn_class = 'btn btn-light text-muted faded';
                    skip_btn_class = 'btn btn-light text-muted faded';
                } else if (anno['labels'][label] === NEG_LABEL) {
                    yes_btn_class = 'btn btn-light text-muted faded';
                    no_btn_class = 'btn btn-danger';
                    skip_btn_class = 'btn btn-light text-muted faded';
                } else if (anno['labels'][label] == NOTSURE_LABEL) {
                    yes_btn_class = 'btn btn-light text-muted faded';
                    no_btn_class = 'btn btn-light text-muted faded';
                    skip_btn_class = 'btn btn-secondary';
                }
            }

            var key = 'controls-' + index;

            controls.push(
                <div key={key} style={{ position: 'relative' }}>
                    <div style={{ textAlign: 'center' }}>
                        <button className={yes_btn_class} style={{ margin: "5px" }} onClick={() => self.setLabel(label, 1)}>
                            {yes_btn_icon} {label}
                        </button>
                        <button className={no_btn_class} style={{ margin: "5px" }} onClick={() => self.setLabel(label, -1)}>
                            {no_btn_icon} Not {label}
                        </button>
                        <button className={skip_btn_class} style={{ margin: "5px" }} onClick={() => self.setLabel(label, 0)}>
                            Not Sure
                        </button>
                    </div>

                    <div style={{ textAlign: 'right', position: 'absolute', right: 0, top: "5px" }}>
                        <button type="button" className="btn btn-info" data-toggle="modal" data-target={'#annotation_guide__' + label}>
                            ?
                        </button>
                    </div>
                    <div id={'annotation_guide__' + label} className="modal" tabIndex="-1" role="dialog">
                        <div className="modal-dialog modal-dialog-centered" role="document">
                            <div className="modal-content">
                                <div className="modal-header">
                                    <h5 className="modal-title">Annotation Guide for "{label}"</h5>
                                    <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                                        <span aria-hidden="true">&times;</span>
                                    </button>
                                </div>
                                <div className="modal-body">
                                    <p dangerouslySetInnerHTML={{ __html: self.getAnnotationGuide(label) }}></p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            );
        });

        if (!this.isBinaryClassification()) {
            // If it's not binary classiciation, add a "Save" button after all the choices were made.
            controls.push(
                <div key='controls-last' style={{ textAlign: 'center', marginTop: '1em' }}>
                    <button className='btn btn-dark' style={{ margin: "5px" }} onClick={() => self.submitResults()}>
                        Save & Next
                    </button>
                </div>
            )
        }


        // ------------------------ Put it all together ------------------------

        return (
            <div style={{ maxWidth: "800px", backgroundColor: "lightblue", padding: "10px 0", borderRadius: "5px", margin: "20px auto" }}>
                <div style={{ margin: '5px', fontFamily: 'monospace', fontSize: '8px', display: 'inline-block', verticalAlign: 'top' }} key="top">
                    file: {req.fname} , line: {req.line_number}
                </div>

                <div style={{ margin: '5px', fontFamily: 'monospace', fontSize: '8px', display: 'inline-block', float: 'right', verticalAlign: 'top' }} key="score">
                    score: {req.score}
                </div>

                <div style={{ margin: '5px', padding: '5px', backgroundColor: "#ecfffe", borderRadius: "5px" }} key="content">
                    {content}
                </div>

                <div style={{ margin: '5px', fontFamily: 'monospace', fontSize: '8px' }} key="meta">
                    {meta}
                </div>

                <div style={{ margin: '5px' }} key="controls">
                    {controls}
                </div>
            </div>
        );
    }
}