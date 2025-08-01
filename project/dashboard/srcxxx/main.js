import "bootstrap/dist/css/bootstrap.min.css"
import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import store from './store/index.js'
import Vue from 'vue'
import axios from 'axios'
import VueSidebarMenu from 'vue-sidebar-menu'
import 'vue-sidebar-menu/dist/vue-sidebar-menu.css'
import VueScreen from 'vue-screen';
import 'v-calendar/dist/style.css';
import { SetupCalendar } from 'v-calendar';






axios.defaults.baseURL = 'http://127.0.0.1:8000'

createApp(App).use(store).use(VueScreen,'bootstrap').use(router, axios).use(VueSidebarMenu).use(SetupCalendar, {}).mount('#app')
import "bootstrap/dist/js/bootstrap.js"
