import axios from 'axios'

const BASE = '/api/v1/status'

export function getStatusSummary() {
  return axios.get(`${BASE}/summary`)
}

export function getStatusNodes() {
  return axios.get(`${BASE}/nodes`)
}
