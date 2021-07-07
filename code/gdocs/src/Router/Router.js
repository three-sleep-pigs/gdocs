import React from 'react';
import { Router, Route, Redirect, Switch } from 'react-router-dom';
import ExcelView from "../View/ExcelView";
import LoginView from "../View/LoginView";
import MenuView from "../View/MenuView";
import {history} from "../Utils/history";

class GDocsRouter extends React.Component{
    constructor(props) {
        super(props);

        history.listen((location, action) => {
            console.log(location,action);
        });
    }

    render() {
        return(
            <Router history={history}>
                <Switch>
                    <Route exact path="/login" component={LoginView}/>
                    <Route exact path="/home" component={MenuView}/>
                    <Route exact path="/excel" component={ExcelView}/>
                    <Redirect from="/*" to="/login"/>
                </Switch>
            </Router>
        );
    }
}

export default GDocsRouter;
