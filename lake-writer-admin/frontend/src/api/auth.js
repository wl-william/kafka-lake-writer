import axios from 'axios'

/**
 * Configure axios to send HTTP Basic credentials with every API request.
 * Credentials are stored in sessionStorage (cleared when browser tab closes).
 */
const AUTH_KEY = 'lw_auth_token'

export function setupAxiosAuth() {
  // Request interceptor: attach Basic auth header
  axios.interceptors.request.use(config => {
    const token = sessionStorage.getItem(AUTH_KEY)
    if (token) {
      config.headers.Authorization = 'Basic ' + token
    }
    return config
  })

  // Response interceptor: redirect to login on 401
  axios.interceptors.response.use(
    response => response,
    error => {
      if (error.response && error.response.status === 401) {
        sessionStorage.removeItem(AUTH_KEY)
        if (window.location.hash !== '#/login') {
          window.location.hash = '#/login'
          window.location.reload()
        }
      }
      return Promise.reject(error)
    }
  )
}

export function login(username, password) {
  const token = btoa(username + ':' + password)
  sessionStorage.setItem(AUTH_KEY, token)
  // Test credentials against a protected endpoint
  return axios.get('/api/v1/status/summary')
}

export function logout() {
  sessionStorage.removeItem(AUTH_KEY)
}

export function isLoggedIn() {
  return !!sessionStorage.getItem(AUTH_KEY)
}
