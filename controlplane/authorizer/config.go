package authorizer

type Config map[string]FunctionPolicy

type FunctionPolicy struct {
	RBAC []string    `yaml:"RBAC"`
	ABAC []ABACRule  `yaml:"ABAC"`
}

type ABACRule struct {
	Argument string `yaml:"argument"`
	Prefix   string `yaml:"prefix"`
}

