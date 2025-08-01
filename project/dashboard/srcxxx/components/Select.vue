<template>

  <select class="form-select form-select-lg mb-3" aria-label=".form-select-lg" v-model="selected" @change="onChange($event)" >
    <option v-for="option in options" v-bind:value="option.value">
      {{ option.text }}
    </option>
  </select>

</template>





<script>

export default {

  data() {
    return {

      selected: 'sm-0001',
      options: [
      { text: 'sm-0001', value: 'sm-0001' },
      { text: 'sm-0002', value: 'sm-0002' },
      { text: 'sm-0003', value: 'sm-0003' },
      { text: 'sm-0009', value: 'sm-0009' },
    ]

    };
  },

  created (){
    this.selected = this.$store.state.selected
  //  let query = this.$route.query
  //  let path = this.$route.path
  //  console.log(this.$route)
    //this.$router.push({path:path, query:{"range":query.range, "dev": this.selected}})

  },
  methods: {
    onChange(event)
    {
      this.$emit("emit-sel", this.selected)
      let dev = this.selected

      let query = this.$route.query
      
      let path = this.$route.path

      //this.$router.push({path:path, query:{"range":query.range, "dev": this.selected}})
      this.$store.commit('setSelect', this.selected)

    }

  },



};
</script>

<style scoped>

</style>
