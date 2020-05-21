const e = React.createElement;

class AnnotationUpdateBoxContainer extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    var boxes = [];

    // For some reason, react converted my array into ab object.
    for (var i in this.props) {
        boxes.push(
            <AnnotationUpdateBox key={i.toString()} {...this.props[i]} />
        )
    }

    return (
      <div>
          {boxes}
      </div>
    );
  }
}