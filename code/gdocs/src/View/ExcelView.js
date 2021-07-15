import React from "react";
import Luckysheet from '../Component/Luckysheet'
class ExcelView extends React.Component{
    render() {
        return(
            <div className="excel-page">
                <header className="App-header">
                    <Luckysheet/>
                </header>
            </div>
        );
    }
}

export default ExcelView;
