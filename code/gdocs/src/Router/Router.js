import React from 'react';
import { BrowserRouter as Router, Route, Redirect, Switch } from 'react-router-dom';
import ExcelView from "../View/ExcelView";
import LoginView from "../View/LoginView";
import RegisterView from "../View/RegisterView";
import MenuView from "../View/MenuView";
import HomeView from "../View/HomeView";
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
                    <Route exact path="/home" component={HomeView} />
                    <Route exact path="/login" component={LoginView}/>
                    <Route exact path="/register" component={RegisterView}/>
                    <Route exact path="/menu" component={MenuView}/>
                    <Route exact path="/excel" component={ExcelView}/>
                    <Route exact path="/" component={HomeView}/>
                    <Redirect from="/*" to="/home"/>
                </Switch>
            </Router>
        );
    }
}

export default GDocsRouter;
