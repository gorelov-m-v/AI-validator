package processor

const (
	StatusSubscribed  = 1
	StatusCalculation = 2
	StatusReceived    = 3
	StatusActive      = 4
	StatusFinished    = 5
	StatusCanceled    = 6
)

var allowedTransitions = map[int]map[int]bool{
	StatusSubscribed: {
		StatusCalculation: true,
		StatusReceived:    true,
		StatusCanceled:    true,
	},
	StatusCalculation: {
		StatusReceived: true,
		StatusCanceled: true,
	},
	StatusReceived: {
		StatusActive:   true,
		StatusCanceled: true,
	},
	StatusActive: {
		StatusFinished: true,
		StatusCanceled: true,
	},
	StatusFinished: {},
	StatusCanceled: {},
}

func ValidateStatusTransition(prevStatus, newStatus int, eventType string) (bool, string) {
	if prevStatus == 0 {
		if eventType == "playerBonusCreate" && newStatus != StatusSubscribed {
			return false, "STATUS_INVALID_INITIAL_STATE: playerBonusCreate requires playerBonusStatus=1 (subscribed)"
		}
		return true, ""
	}

	if prevStatus == newStatus {
		return true, ""
	}

	if allowed, exists := allowedTransitions[prevStatus]; exists {
		if allowed[newStatus] {
			return true, ""
		}
	}

	return false, "STATUS_INVALID_TRANSITION"
}
