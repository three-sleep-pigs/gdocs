import React from "react";
import '../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css';
import '../CSS/HomeView.css';
import {withRouter} from "react-router-dom";
class HomeView extends React.Component{
    render() {
        return(
            <div className="position-relative overflow-hidden p-3 p-md-5 m-md-3 text-center bg-light">
                <div className="col-md-5 p-lg-5 mx-auto my-5">
                    <h1 className="display-4 font-weight-normal">Welcome</h1>
                    <p className="lead font-weight-normal">This is naive gDocs.</p>
                    <a className="btn btn-outline-secondary" href="/register">Register</a>
                    <a className="btn btn-outline-secondary" href="/login"> Login </a>
                </div>
                <div className="product-device shadow-sm d-none d-md-block"/>
                <div className="product-device product-device-2 shadow-sm d-none d-md-block"/>
            </div>
        );
    }
}

export default withRouter(HomeView);
