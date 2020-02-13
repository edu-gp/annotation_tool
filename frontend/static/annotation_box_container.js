const e = React.createElement;

class AnnotationBoxContainer extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    var boxes = [];

    // For some reason, react converted my array into ab object.
    for (var i in this.props) {
        data=this.props[i]
        boxes.push(
            <AnnotationBox key={i.toString()} {...data} />
        )
    }

    return (
      <div>
          {boxes}
      </div>
    );
  }
}