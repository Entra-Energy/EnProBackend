<template>

<div class="row">
  <div class= 'col-sm-6 d-flex d-sm-block'>
    <ul class="list-group list-group-horizontal">
      <li
        v-for="(item, index) in items"
        :key="item.index"
        :class="{ active: activeIndex === index }"
        @click="setActive(index, item.title);clickHandler()"
        >
        <slot :item="item">

            {{ item["title"] }}
            {{ activeI }}

        </slot>
      </li>
    </ul>
  </div>
</div>

</template>


<script>

export default {

  data() {
    return {
      activeIndex: 0,
      title: 'today',
      items: [
        {
          id: 1,
          title: "today",
        },
        {
          id: 2,
          title: "month",
        },
        {
          id: 3,
          title: "year",
        },
      ],
    };
  },
  props: {
    activeI: {
      type: Number,
    },
  },
  created (){
      let path = this.$route.path
      if (path == '/dashboard')
      {
        let test = this.$store.state.dash_init

        if (test == 'today')
        {
          this.activeIndex = 0
        }

        if (test == 'month')
        {
          this.activeIndex = 1
        }
        if (test == 'year')
        {
          this.activeIndex = 2
        }

      }
      if (path == '/client')
      {
        let test = this.$store.state.client_init
        if (test == 'today')
        {
          this.activeIndex = 0
        }

        if (test == 'month')
        {
          this.activeIndex = 1
        }
        if (test == 'year')
        {
          this.activeIndex = 2
        }
      }

      //this.activeIndex = this.$store.state.count
      //this.title = this.$store.state.path


      // let path = this.$route.path
      // this.$router.push({path:path, query:{"range":this.title}})
      //console.log(this.title)

  },
  methods: {
    setActive(index, title) {
      this.activeIndex = index;
      this.$store.commit('increment',title)
      this.$emit("emit-it", title)
      let path = this.$route.path
      this.$store.commit('setPath', title)
      if (path == '/dashboard')
      {
        
        this.$store.commit('initDash', title)
      }
      if (path == '/client')
      {
        this.$store.commit('initClient', title)
      }
    },
    clickHandler(index){
     //let query = {"range":index}
     //this.$emit("emit-it", query)
  },

  }

};
</script>

<style scoped>
.active {
  font-weight: bold;
  color: #fff;
  background-color: #772685!important;
  box-shadow: 2px 2px 6px rgb(0 0 0 / 40%);
  border: none;
  /*border-color: #e14eca!important;*/
  outline: 0!important;
  border-bottom-right-radius: 0;
  border-top-right-radius: 0;
}
ul li {
    color: #e14eca;
    border-color: #e14eca;
    background: transparent;
    padding: 4px 14px;
    cursor: pointer;
    border-radius: 0.2857rem;
    line-height: 1.35;
    text-align: center;
    vertical-align: middle;
    font-weight: 600;
    font-size: .75rem;
    border: 1px solid #e14eca!important;
}
ul {
  list-style-type: none;
  margin-bottom: 15px;

}
ul.list-group {
    float: left;
}
ul li:nth-child(1) {
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
ul li:nth-child(2) {
  border-radius:0;
}
ul li:nth-child(3) {
  border-left:none !important;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
}
ul li:nth-child(3).active {
  border-left:none !important;
  border-top-right-radius: 0.2857rem;
  border-bottom-right-radius: 0.2857rem;
}
</style>
