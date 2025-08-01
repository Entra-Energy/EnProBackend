import { createStore } from 'vuex'

export default createStore({
  state: {
    count:'today',
    path: 'today',
    selected:'',
    zoom: {},
    checked_devs: '',
    dash_init: 'today',
    client_init: 'today',
    dash_zoom: {},
    client_zoom: '',



  },
  mutations: {
    increment (state, n) {
      state.count = n
    },
    setPath (state,query)
    {
      state.path = query
    },
    setSelect (state, select)
    {
      state.selected = select
    },
    setZoom (state, myZoom)
    {
      state.zoom = myZoom
    },
    setChecked (state, checked_state)
    {
      state.checked_devs = checked_state
    },
    initDash(state,dash)
    {
      state.dash_init = dash
    },
    initClient(state, client)
    {
      state.client_init = client
    },
    dashZoomInit(state, dashZ)
    {
      state.dash_zoom = dashZ
    },
    clientZoomInit(state, clientZ)
    {
      state.client_zoom = clientZ
    }


  },
  actions: {
  },
  modules: {
  }
})
