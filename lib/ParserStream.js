const concat = require('concat-stream')
const jsonld = require('jsonld')
const Readable = require('readable-stream')
const rdf = require('@rdfjs/data-model')
const RdfTerms = require('rdf-terms')

class ParserStream extends Readable {
  constructor (input, options) {
    super({
      objectMode: true,
      read: () => {}
    })

    options = options || {}

    this.baseIRI = options.baseIRI || ''
    this.context = options.context
    this.factory = options.factory || rdf
    this.blankNodes = {}

    const concatStream = concat({encoding: 'string'}, (data) => {
      if (!data) {
        this.push(null)

        return
      }

      this.parse(data).then(() => {
        this.push(null)
      }).catch((err) => {
        this.emit('error', err)
      })
    })

    input.pipe(concatStream)

    input.on('error', (err) => {
      this.emit('error', err)
    })
  }

  /**
   * Function rewrite with term.termType.
   * This is done to fix issue #243, see https://github.com/digitalbazaar/jsonld.js/issues/243 .
   * **/
  term (term) {
    switch (term.termType) {
      case 'NamedNode':
        return this.factory.namedNode(term.value)
      case 'BlankNode':
        return this.factory.blankNode(term.value.substr(2)) // Remove the '_:' prefix. see https://github.com/digitalbazaar/jsonld.js/issues/244
      case 'Literal':
        return this.factory.literal(term.value, term.language || term.datatype)
      case 'DefaultGraph':
        return this.factory.defaultGraph()
    }
  }

  parse (data) {
    return ParserStream.toJson(data).then((json) => {
      // forward context as prefixes if available
      if ('@context' in json) {
        Object.keys(json['@context']).forEach((prefix) => {
          this.emit('prefix', prefix, this.factory.namedNode(json['@context'][prefix]))
        })
      }

      const toRdfOptions = {base: this.baseIRI}

      // use context from options if given
      if (this.context) {
        toRdfOptions.expandContext = this.context
      }

      return jsonld.promises().toRDF(json, toRdfOptions)
    }).then((rawGraph) => {
      for (let index in rawGraph) {
        this.push(RdfTerms.mapTerms(rawGraph[index], (value) => this.term(value), this.factory))
      }
    }).catch((error) => {
      this.emit('error', error)
    })
  }

  static toJson (data) {
    if (typeof data === 'string') {
      return new Promise((resolve, reject) => {
        try {
          resolve(JSON.parse(data))
        } catch (err) {
          reject(err)
        }
      })
    } else if (typeof data === 'object') {
      return Promise.resolve(data)
    } else {
      return Promise.reject(new Error('unknown type'))
    }
  }
}

module.exports = ParserStream
