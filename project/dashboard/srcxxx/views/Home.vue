<template>

    <!-- <img alt="Vue logo" src="../assets/logo.png"> -->
    <div class='row'><div class='col'><Range @emit-it="handleChange"/></div></div>
    <div class='row'><div class='col'><LineCharts :query="query" /></div></div>
    <div class='row'><div class='col'><PriceChart :query="query" /></div></div>
    <div class='row'><div class='col'><Table /></div></div>


</template>

<script>

// @ is an alias to /src

import Range from '@/components/Range.vue'
import Table from '@/components/Table.vue'
import LineCharts from '@/components/LineCharts.vue'
import PriceChart from '@/components/PriceChart.vue'


export default {
  name: 'Home',
  components: {
    Range,
    Table,
    LineCharts,
    PriceChart
  },
  data() {
    return {

      query:"today",
      rangeTitle:'',

    }
  },
  created (){
    let rangeIndex = this.rangeTitle
    let path = this.$route.path
    //let title = this.$store.state.path

    this.$router.push({path:path, query:{"range":rangeIndex}})


  },

  methods: {
    handleChange(payload){
      //console.log(payload)
      let path = this.$route.path
      this.query = payload
      this.$router.push({path:path, query:{"range":payload}})

    }


  },

  watch: {
    '$store.state.dash_init': {
      immediate: true,
      handler() {
          this.rangeTitle = this.$store.state.dash_init;


      }
    }
  },



}
</script>
