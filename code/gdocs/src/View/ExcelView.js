import React from "react";
import Luckysheet from '../Component/Luckysheet'
import {withRouter} from "react-router-dom";
class ExcelView extends React.Component{
    constructor(props) {
        super(props);
    }
    render() {
        return(
            <div className="excel-page">
                <header className="App-header">
                    <Luckysheet id={this.props.location.state.id} filename={this.props.location.state.filename}/>
                </header>
            </div>
        );
    }
}

export default ExcelView;
