(ns com.iraqidata.tasks.day-2
  (:require
   [clojure.string :as str]
   '[scicloj.clay.v2.api :as clay]
   [tablecloth.api :as tc]
   tech.v3.datatype.casting))

(def ds (tc/dataset "./resources/data/flights.csv"
                    {:key-fn #(keyword (str/replace (name %) "_" "-"))}))

;; 1. How many flights were there in total?
(def flights-total
  (tc/row-count ds))
flights-total

;; 2. How many unique carriers were there in total?
(def total-unique-carriers
  (tc/row-count (tc/unique-by ds :carrier))
  )
total-unique-carriers

;; 3. How many unique airports were there in total?
(def total-unique-airports
  (-> ds
      (tc/select-columns [:origin :dest])
      (tc/unique-by)
      (tc/row-count)))
total-unique-airports
;; without thread
;; (def total-unique-airports
;; (tc/row-count
;;  (tc/unique-by ds [:origin :dest])))
;; total-unique-airports

;; 4. What is the average arrival delay for each month?
(def avg-arrival-delay-by-month
 ( -> ds
  (tc/group-by [:month])
  (tc/mean :arr-delay))


;; Optional: Use the `airlines` dataset to get the name of the carrier with the
;; highest average distance.

(def airlines
  (tc/dataset "./resources/data/airlines.csv"
              {:key-fn keyword}))
  
(def carrier-highest-avg-distance
  (-> ds
      (tc/group-by [:carrier])
      (tc/mean :distance)
      (tc/inner-join airlines {:on [:carrier :carrier]})
      (tc/order-by [:avg-distance] :desc)
      (tc/select-columns [:name :avg-distance])
      (tc/first)))
 carrier-highest-avg-distance