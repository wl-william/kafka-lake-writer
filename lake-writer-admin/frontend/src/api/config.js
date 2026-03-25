import axios from 'axios'

const BASE = '/api/v1/configs'

export function listConfigs(params) {
  return axios.get(BASE, { params })
}

export function getConfig(id) {
  return axios.get(`${BASE}/${id}`)
}

export function createConfig(data) {
  return axios.post(BASE, data)
}

export function updateConfig(id, data) {
  return axios.put(`${BASE}/${id}`, data)
}

export function deleteConfig(id) {
  return axios.delete(`${BASE}/${id}`)
}

export function pauseConfig(id) {
  return axios.put(`${BASE}/${id}/pause`)
}

export function resumeConfig(id) {
  return axios.put(`${BASE}/${id}/resume`)
}

export function getChangelog(id) {
  return axios.get(`${BASE}/${id}/changelog`)
}

export function validateConfig(data) {
  return axios.post(`${BASE}/validate`, data)
}
