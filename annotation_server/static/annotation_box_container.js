const e = React.createElement;

class AnnotationBoxContainer extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    var boxes = [];

    // For some reason, react converted my array into ab object.
    for (var i in this.props) {
        boxes.push(
            <AnnotationBox key={i.toString()} {...this.props[i]} />
        )
    }

    return (
      <div>
          {boxes}
      </div>
    );
  }
}