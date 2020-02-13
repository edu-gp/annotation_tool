const e = React.createElement;

class AnnotationBox extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    var content = [];

    if (this.props.pattern_info !== undefined) {
        let tokens = this.props.pattern_info.tokens;
        let matches = this.props.pattern_info.matches;

        var match_idx = 0;
        var rendered_tokens = [];

        for (var i = 0; i < tokens.length; i++) {
            if (match_idx < matches.length && matches[match_idx][0] == i) {
                // Current token was matched
                rendered_tokens.push(
                    <span style={{padding: '3px', display: 'inline-block', backgroundColor: "yellow", color: "black" }} key={i.toString()}>{tokens[i]}</span>
                );
                match_idx = match_idx + 1;
            } else {
                rendered_tokens.push(
                    <span style={{padding: '3px', display: 'inline-block' }} key={i.toString()}>{tokens[i]}</span>
                )
            }
        }

        content.push(
            <div style={{padding: '5px', backgroundColor: "#ecfffe", borderRadius: "5px"}} key='rendered_tokens'>
                {rendered_tokens}
            </div>
        );
    } else {
        content.push(
            <div key='rendered_tokens'>
                {this.props.data.text}
            </div>
        );
    }

    var meta = []

    let _meta = this.props.data.meta;
    for (var k in _meta) {
        if (_meta.hasOwnProperty(k)) {
            meta.push(
                <div key={k}>
                    <span>{k}</span>: <span>{_meta[k]}</span>
                </div>
            );
        }
    }

    var controls = []

    controls.push(
        <div key='controls' style={{textAlign: 'center'}}>
            <button className='green_button' style={{margin: "5px"}} onClick={() => this.setState({ liked: true })}>
                Accept
                {/* <br />
                (a) */}
            </button>
            <button className='red_button' style={{margin: "5px"}} onClick={() => this.setState({ liked: true })}>
                Deny
                {/* <br />
                (x) */}
            </button>
            <button className='gray_button' style={{margin: "5px"}} onClick={() => this.setState({ liked: true })}>
                Skip
                {/* <br />
                (space) */}
            </button>
        </div>
    );

    return (
      <div style={{ maxWidth: "800px", backgroundColor: "lightblue", padding: "10px 0", borderRadius: "5px", margin: "20px auto" }}>
        <div style={{margin: '5px', fontFamily: 'monospace', fontSize: '8px', display: 'inline-block', verticalAlign: 'top'}} key="top">
            file: {this.props.fname} , line: {this.props.line_number}
        </div>
        
        <div style={{margin: '5px', fontFamily: 'monospace', fontSize: '8px', display: 'inline-block', float: 'right', verticalAlign: 'top'}} key="score">
            score: {this.props.score}
        </div>

        <div style={{margin: '5px'}} key="content">
            {content}
        </div>

        <div style={{margin: '5px', fontFamily: 'monospace', fontSize: '8px'}} key="meta">
            {meta}
        </div>

        <div style={{margin: '5px'}} key="controls">
            {controls}
        </div>
    </div>
    );
  }
}