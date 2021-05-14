const App = require('./lib/App.js')
App.app = () => {
  if(!global.liveChangeFrameworkApp) {
    global.liveChangeFrameworkApp = new App()
  }
  return global.liveChangeFrameworkApp
}
module.exports = App