package compute

type Query struct {
	commandID CommandID
	arguments []string
	argCount  int
	arg0      string
	arg1      string
	arg2      string
	arg3      string
	arg4      string
}

func NewQuery(commandID CommandID, arguments []string) Query {
	query := Query{
		commandID: commandID,
		arguments: arguments,
		argCount:  len(arguments),
	}

	query.setArgumentSlots(arguments)

	return query
}

func (c *Query) CommandID() CommandID {
	return c.commandID
}

func (c *Query) Arg(index int) string {
	switch index {
	case 0:
		return c.arg0
	case 1:
		return c.arg1
	case 2:
		return c.arg2
	case 3:
		return c.arg3
	case 4:
		return c.arg4
	default:
		if index >= 0 && index < len(c.arguments) {
			return c.arguments[index]
		}

		return ""
	}
}

func (c *Query) Arguments() []string {
	if c.arguments != nil {
		return c.arguments
	}

	switch c.argCount {
	case 0:
		return []string{}
	case 1:
		return []string{c.arg0}
	case 2:
		return []string{c.arg0, c.arg1}
	case 3:
		return []string{c.arg0, c.arg1, c.arg2}
	case 4:
		return []string{c.arg0, c.arg1, c.arg2, c.arg3}
	case 5:
		return []string{c.arg0, c.arg1, c.arg2, c.arg3, c.arg4}
	default:
		return nil
	}
}

func NewQueryFromSlots(commandID CommandID, argCount int, arg0, arg1, arg2, arg3, arg4 string) Query {
	return Query{
		commandID: commandID,
		argCount:  argCount,
		arg0:      arg0,
		arg1:      arg1,
		arg2:      arg2,
		arg3:      arg3,
		arg4:      arg4,
	}
}

func (c *Query) setArgumentSlots(arguments []string) {
	if len(arguments) > 0 {
		c.arg0 = arguments[0]
	}
	if len(arguments) > 1 {
		c.arg1 = arguments[1]
	}
	if len(arguments) > 2 {
		c.arg2 = arguments[2]
	}
	if len(arguments) > 3 {
		c.arg3 = arguments[3]
	}
	if len(arguments) > 4 {
		c.arg4 = arguments[4]
	}
}

func (c *Query) ArgumentCount() int {
	if c.arguments != nil {
		return len(c.arguments)
	}

	return c.argCount
}

func (c *Query) VariableArguments() []string {
	return c.arguments
}
