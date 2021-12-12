function nonEmpty(value) {
  if(!value) return 'empty'
  if(typeof value == 'string') {
    if(!value.trim()) return 'empty'
  }
  if(Array.isArray(value)) {
    if(value.length == 0) return 'empty'
  } else if(value instanceof Date) {
    return
  } if(typeof value == 'object') {
    if(Object.keys(value).length == 0) return 'empty'
  }
}

function getField(context, fieldName) {
  const propPath = context.propName ? context.propName.split('.') : []
  propPath.pop()
  let path
  if(fieldName[0] == '/') {
    path = fieldName.slice(1).split('.')
  } else {
    path = propPath.concat(fieldName.split('.'))
  }
  let p = context.props
  for(let part of path) p = p[part]
  return p
}

nonEmpty.isRequired = () => true

let validators = {
  nonEmpty: (settings) => nonEmpty,

  minLength: ({ length }) => (value) => value.length < length ? 'tooShort' : undefined,
  maxLength: ({ length }) => (value) => value.length > length ? 'tooLong' : undefined,

  elementsNonEmpty: (settings) => (value) => {
    if(!value) return
    for(let el of value) {
      if(nonEmpty(el)) return 'someEmpty'
    }
  },

  minTextLength: ({ length }) =>
      (value) => (typeof value == 'string')
      && value.replace(/<[^>]*>/g,'').length < length ? 'tooShort' : undefined,
  maxTextLength: ({ length }) =>
      (value) => value && value.replace(/<[^>]*>/g,'').length > length ? 'tooLong' : undefined,
  nonEmptyText: (settings) => (value) => {
    if(!value) return 'empty'
    if(typeof value != 'string') return 'empty'
    value = value.replace(/<[^>]*>/g, "")
    if(!value.trim()) return 'empty'
  },

  ifEq: ({ prop, to, then }, { getValidator }) => {
    let validators = then.map(getValidator)
    const validator = (value, context) => {
      if(getField(context, prop) == to) {
        for(let v of validators) {
          const err = v(value, context)
          if(err) return err
        }
      }
    }
    validator.isRequired = (context) => {
      if(getField(context, prop) == to) {
        for(let v of validators) {
          if(v.isRequired && v.isRequired(context)) return true
        }
        return false
      }
    }
    return
  }

}

module.exports = validators