<template>
  <div class="about">
    <div class="row">
      <div class="col-md-12">
          <Range @emit-it="handleChange" />
      </div>
    </div>
    <div class="row">
      <div class="col-md-12">
        <div class="pull-left">
          <Select @emit-sel="selectChange" />
      </div>
    </div>
   </div>
   <div class='row'><div class='col'><LineCharts :query="query" /></div></div>



  </div>
</template>
<script>
// @ is an alias to /src
import Range from '@/components/Range.vue'
import Select from '@/components/Select.vue'
import LineCharts from '@/components/LineCharts.vue'

export default {
  name: 'About',
  components: {
    Range,
    Select,
    LineCharts,
  },
  data() {
    return {
      dev : {},
      query:{},
      rangeTitle: ''

    }
  },
  created() {
    let rangeIndex = this.rangeTitle
    let path = this.$route.path
    //let title = this.$store.state.path

    this.$router.push({path:path, query:{"range":rangeIndex}})

    // let path = this.$route.path
    // let title = this.$store.state.path
    let selected = this.$store.state.selected

    // this.query["range"] = title
    // this.query["dev"] = selected
    // console.log(this.query)
    // // //
    // this.query['range'] = title
    // if (selected){
    //   this.query['dev'] = selected
    // }
    //
    // this.$router.push({path:path, query:this.query})



  },
  methods: {

    handleChange(payload){

      let ran = payload
      //console.log(ran)
      let path = this.$route.path

      this.query["range"] = payload
      //
      // //

      //
      //
      // //console.log(this.query)
      this.$router.push({path:path, query:this.query})
      // console.log(this.query)
    //  this.$router.push({path:path, query:{"range":payload.range, "dev": this.dev.dev}})
    //  console.log(payload)
  },
    selectChange(value)
    {
       let path = this.$route.path
      //
      let dev = value
      let range = this.$store.state.path
      this.query["range"] = range

      this.query["dev"] = dev
      
      // //let query = this.$route.query
      // //this.dev = value
      //
      // console.log(this.query)
      this.$router.push({path:path, query:this.query})
      //this.$router.push({path:path,query:this.query})

    }
  },
  watch: {
    '$store.state.client_init': {
      immediate: true,
      handler() {
          this.rangeTitle = this.$store.state.client_init;


      }
    }
  },
}
</script>

<style scoped>
.pull-left {
    float: left;
}
</style>
