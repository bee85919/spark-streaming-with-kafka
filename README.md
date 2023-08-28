### Scenario
1. mobile 앱에서 광고를 요청하면, 광고 데이터를 받는다.
2. 광고를 유저가 보면 impression data 를 발생시킨다.
3. impression 데이터에는 광고를 식별하기 위한 주요정보가 담겨있다. <br>
a. impId <br>
b. requestId <br>
c. adId <br>
d. userId <br>
e. deviceId <br>
f. inventoryId <br>
4. 광고를 유저가 click 하면 click 데이터를 발생시킨다.
5. click 데이터는 impId 와 함께 꼭 필요한 정보만 남긴다. <br>
a. impId
b. clickUrl
6. 정상 impression 은 다음 조건으로 판단한다.
a. 한 번 발생한 impId 는 impValidWindow 기간동안 하나만 유효하다.
b. impValidWindow 는 정책에 따라 설정 가능하다.
7. 정상 click event 는 다음 조건으로 판단한다. <br>
a. 같은 impId 인 impression 이 impValidWindow 기간동안 발생한적 있어야 한다. <br>
b. 같은 impId 로 발생한 click 는 clickDedupWindow 기간동안 하나만 유효하다. <br>
c. impValidWindow, clickDedupWindow 는 정책에 따라 설정 가능하다. <br>
8. 실시간으로 정상 click event 는 adId 기준으로 count 한 결과를 확인할 수 있어야 한다.
9. event 는 전송된 시점부터 500ms 이내에 count 에 반영되어야 한다.
