import http from "k6/http";
export const options = {
  vus: 30,
  duration: "10s",
  summaryTrendStats: ['avg','med','p(99)','max'],
};
export default function () {
  http.get("http://localhost:8080/receive");
}
