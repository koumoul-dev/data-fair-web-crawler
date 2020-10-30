const config = require('config')
const url = require('url')
const shortHash = require('short-hash')
const puppeteer = require('puppeteer')
const cheerio = require('cheerio')
const FormData = require('form-data')
const debug = require('debug')('df-web-crawler')

debug(process.env.NODE_ENV, config)

const axios = require('axios').create({
  baseURL: config.dataFair.url + '/api/v1/',
  headers: { 'x-apiKey': config.dataFair.apiKey }
})
const baseUrl = config.baseUrl

const schema = [
  { key: 'path', type: 'string', 'x-refersTo': 'http://schema.org/DigitalDocument' },
  { key: 'title', type: 'string', 'x-refersTo': 'http://www.w3.org/2000/01/rdf-schema#label' },
  { key: 'url', type: 'string', 'x-refersTo': 'https://schema.org/WebPage' },
  { key: 'tags', type: 'string', separator: ',', 'x-refersTo': 'https://schema.org/DefinedTermSet' }
]

const history = []
const frontier = [baseUrl]

async function main () {
  console.log('init dataset', config.dataset)
  /* try {
    await axios.delete(`datasets/${config.dataset.id}`)
  } catch (err) {
    // nothing
  } */

  const dataset = (await axios.put(`datasets/${config.dataset.id}`, {
    title: config.dataset.title,
    description: config.dataset.description,
    isRest: true,
    schema
  })).data
  debug('dataset created/updated', dataset)
  await new Promise(resolve => setTimeout(resolve, 2000))

  console.log('start at', baseUrl)

  const browser = await puppeteer.launch()
  const page = await browser.newPage()

  while (frontier.length) {
    const next = frontier.shift()
    debug('crrawl', next)
    const nextUrl = new URL(next)

    // if we are a link to a section, also crawl the whole page
    if (nextUrl.hash) {
      const nextNoHash = next.replace(nextUrl.hash, '')
      if (!history.includes(nextNoHash)) {
        history.push(nextNoHash)
        // frontier.push(hrefUrl)
        frontier.unshift(nextNoHash)
      }
    }
    await page.goto(next, { waitUntil: 'networkidle0' })
    const html = await page.content()
    const $ = cheerio.load(html)

    $('a').each(function (i, elem) {
      const href = $(this).attr('href')
      // console.log('link found', href)
      const hrefUrl = url.resolve(baseUrl, href)
      if (hrefUrl.startsWith(baseUrl) && !history.includes(hrefUrl)) {
        if (hrefUrl.hash) {

        } else {
          history.push(hrefUrl)
          // frontier.push(hrefUrl)
          frontier.unshift(hrefUrl)
        }
      }
    })

    if (config.blacklist && config.blacklist.includes(next)) continue

    if (config.transform.prune) {
      config.transform.prune.forEach(s => {
        $(s).remove()
      })
    }

    const tags = []
    let outputHtml
    let title = $('title').text()
    if (nextUrl.hash) {
      const targetElement = $(nextUrl.hash)
      for (const fragmentDef of config.transform.fragments) {
        const fragment = fragmentDef.wrapper ? targetElement.closest(fragmentDef.wrapper) : targetElement
        const fragmentHtml = fragment.html()
        if (fragmentHtml) {
          if (fragmentDef.title) title = fragment.find(fragmentDef.title).text() || title
          else title = targetElement.text() || title
          outputHtml = fragmentHtml
          tags.push(fragmentDef.tag)
          break
        }
      }
    }
    outputHtml = outputHtml || `<html>${$('html').html()}</html>`

    const form = new FormData()
    title = title.trim()
    if (config.transform.titlePrefix && title.startsWith(config.transform.titlePrefix)) {
      title = title.replace(config.transform.titlePrefix, '')
    }
    form.append('title', title)
    form.append('url', next)
    if (tags.length) form.append('tags', tags.join(','))
    const data = Buffer.from(outputHtml)
    const dataOpts = {
      contentType: 'text/html',
      knownLength: data.length,
      filename: 'content.html'
    }
    form.append('attachment', data, dataOpts)
    const headers = {
      ...form.getHeaders(),
      'content-length': form.getLengthSync()
    }
    await axios({
      method: 'put',
      url: `datasets/${config.dataset.id}/lines/${encodeURIComponent(shortHash(next))}`,
      data: form,
      headers
    })
  }
  await browser.close()
}

main()
  .then(() => process.exit())
  .catch(err => { console.error((err.response && err.response.data) || err); process.exit(-1) })
