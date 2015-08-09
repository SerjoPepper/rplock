module.exports = (grunt) ->
  grunt.loadNpmTasks('grunt-coffeelint')
  grunt.loadNpmTasks('grunt-contrib-coffee')

  grunt.initConfig {
    coffeelint: {
      all: ['src/**/*.coffee']
      options: {
        configFile: 'coffeelint.json'
      }
    }
    coffee: {
      compile: {
        files: {
          'lib/rplock.js': 'src/rplock.coffee'
        }
      }
    }
  }