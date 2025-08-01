<template>

  <!-- <Calendar /> -->
  <!-- <DatePicker v-model="range" is-range /> -->

 <!-- <div class="container-fluid">
      <div class="row flex-nowrap">
          <div class="col-auto col-md-3 col-xl-2 px-sm-2 px-0 bg-dark">
              <div class="d-flex flex-column align-items-center align-items-sm-start px-3 pt-2 text-white min-vh-100">

                <sidebar-menu :menu="menu" :relative=true :hideToggle=true  />

              </div>
          </div>
          <div class="col py-3">
              <router-view/>
          </div>
      </div>
  </div>
<div class="container-fluid">
    <div class="row flex-nowrap">
<sidebar-menu :menu="menu" :relative=false  />
<div class="row">
      <router-view/>
    </div>
    </div>

  </div>



    <router-view/> -->

    <div class='wrapper'>
      <div class="sidebar">

        <sidebar-menu :menu="menu" :collapsed="collapsed" :disableHover="disableHover" :hideToggle="hideToggle" />

      </div>
      <div class='main-panel'>
        <nav class='navbar navbar-expand-lg navbar-absolute navbar-transparent'>

          <img src="@/assets/logo.png" />

        </nav>
        <div class='content'>
          <router-view/>
        </div>
        <footer class='footer'></footer>

      </div>

    </div>


    <!-- <router-link to="/dashboard">Home</router-link> |
    <router-link to="/about">About</router-link>
    <sidebar-menu :menu="menu" :relative=false  />
      <router-view/> -->



</template>
<script>
/**/
import { Calendar, DatePicker } from 'v-calendar';



export default {

// setup() {
//  const screen = useScreen()
//  const grid = useGrid('tailwind')
//
//  return {
//    screen,
//    grid,
//  }
// },
    data() {
      return {

        menu: [
          {
            header: 'Energy Dashboard',
            hiddenOnCollapse: true
          },
          {
            href: '/',
            title: 'Dashboard',
            icon: 'fa-solid fa-chart-line',

          },
          {
            href: '/client',
            title: 'Client',
            icon: 'fa-solid fa-display',

          },
          {
            href: '/system',
            title: 'System',
            icon: 'fa-solid fa-display',

          },
          {
            href: '/grid',
            title: 'Flexability Simulate',
            icon: 'fa-solid fa-display',

          },
          {
            href: '/flexi',
            title: 'Flexability',
            icon: 'fa-solid fa-display',

          },

        ],
        collapsed: false,
        disableHover: true,
        hideToggle: true,


      }
    },

    methods: {

      detectIt(){

        if (this.$screen.width < 1000)
        {
          this.collapsed = true
        }
        else {
          this.collapsed = false
        }
      },

    },
    created () {

     this.detectIt()
    },
    computed: {

    },
  }

</script>
<style lang="scss">
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
}

#nav {

  padding: 30px;

  a {
    font-weight: bold;
    color: #2c3e50;

    &.router-link-exact-active {
      color: #42b983;
    }
  }
}
.v-sidebar-menu {
    background-color: #782686 !important;
    top: 53px !important;
    bottom: 37px !important;
    border-bottom-right-radius: 0.2857rem;
    border-top-right-radius: 0.2857rem;
}
/*****/
.sidebar {
  height: calc(100vh - 90px);
  width: 230px;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1;
  display: block;
  box-shadow: 0 0 45px 0 rgb(0 0 0 / 60%);
  margin-top: 52px;
  margin-left: 20px;
  border-radius: 5px;
  transition: .5s cubic-bezier(.685,.0473,.346,1);
}
.main-panel{
  position: relative;
  float: right;
  width: 100%;
  min-height: 100vh;
  border-top: 2px solid #e14eca;
  background: linear-gradient(#1e1e2f,#1e1e24);
}
.main-panel>.navbar {
    margin-bottom: 0;
}
.main-panel>.content {
    padding: 0px 30px 30px 280px;
    min-height: calc(100vh - 70px);
}
.v-sidebar-menu.vsm_expanded {
  max-width:250px !important;
}
@media screen and (max-width: 991px){
.main-panel .content {
    padding-left: 100px;
}
.sidebar {
    position: fixed;
    display: block;
    top: 0;
    height: 100%;
    max-width: 68px;
    right: auto;
    left: 0;
    margin: 0;
    border-radius: 0;
    z-index: 1032;
    visibility: visible;
    overflow-y: visible;
    padding: 0;
//     -webkit-transition: .5s cubic-bezier(.685,.0473,.346,1);
//     transition: .5s cubic-bezier(.685,.0473,.346,1);
//      // -webkit-transform: translate3d(-100px,0,0);
//      // transform: translate3d(-100px,0,0);
//      -webkit-transform: width(70px);
//      transform: width(70px);
 }
}
.navbar{
padding-bottom: 0.625rem;
min-height: 53px;
/*border-bottom: 1px solid #ddd;*/
}
.v-sidebar-menu .vsm--header {
    font-size: 16px;
    font-weight: 600;
    padding: 6px 15px;
    text-transform: none;
    white-space: nowrap;
    border-top-right-radius: 0.2857rem;
    border-bottom: 1px solid;
}
.v-sidebar-menu .vsm--mobile-bg {
  background: none !important;
}
.v-sidebar-menu .vsm--link_level-1 .vsm--icon {
    background: none !important;
}



</style>
