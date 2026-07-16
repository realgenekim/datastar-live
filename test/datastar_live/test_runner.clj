(ns datastar-live.test-runner
  (:require
   [clojure.test :as test]
   [datastar-live.core-test]
   [datastar-live.socket-test]))

(defn -main [& _]
  (let [{:keys [fail error]}
        (test/run-tests 'datastar-live.core-test 'datastar-live.socket-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
