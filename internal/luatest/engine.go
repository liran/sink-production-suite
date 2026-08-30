// Package luatest runs merge programs with the same Lua implementation used by Sink.
package luatest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

const mergeTimeout = 5 * time.Second

type Engine struct {
	luaVM      *vm.VM
	merge      vm.Value
	bridge     *jsonBridge
	observedAt string
}

func New(source []byte) (*Engine, error) {
	block, err := parser.Parse("merge.lua", string(source))
	if err != nil {
		return nil, err
	}
	program, err := compiler.Compile("merge.lua", block)
	if err != nil {
		return nil, err
	}
	limits := vm.Limits{
		MaxCallDepth:    256,
		MaxStackSlots:   65_536,
		MaxInstructions: 1_000_000,
	}
	options := []vm.VMOption{vm.WithLimits(limits)}
	luaVM := vm.New(options...)
	stdlib.Open(luaVM)
	addUnicodeTextFunctions(luaVM)
	bridge := newJSONBridge(luaVM)
	engine := &Engine{luaVM: luaVM, bridge: bridge}
	if err := engine.openSinkV1(); err != nil {
		luaVM.Close(context.Background())
		return nil, err
	}
	results, err := luaVM.Run(program)
	if err != nil {
		luaVM.Close(context.Background())
		return nil, err
	}
	if len(results) != 1 || !results[0].IsFunction() {
		luaVM.Close(context.Background())
		return nil, errors.New("merge program did not return one function")
	}
	engine.merge = results[0]
	return engine, nil
}

func addUnicodeTextFunctions(luaVM *vm.VM) {
	value := luaVM.GetGlobal("utf8")
	if !value.IsTable() {
		panic("Lua UTF-8 library is unavailable")
	}
	library, ok := value.AsTable().(*vm.Table)
	if !ok {
		panic("Lua UTF-8 library is not a concrete table")
	}
	library.SetString("upper", vm.NewNativeFunc(unicodeUpper))
}

func unicodeUpper(state *vm.VM) int {
	value := state.Get(1)
	if !value.IsString() {
		got := "no value"
		if state.ArgCount() >= 1 {
			got = value.Type()
		}
		panic(fmt.Sprintf("bad argument #1 to 'utf8.upper' (string expected, got %s)", got))
	}
	state.Set(0, vm.NewString(strings.ToUpper(value.AsString())))
	return 1
}

func (e *Engine) Close() {
	if e == nil || e.luaVM == nil {
		return
	}
	e.luaVM.Close(context.Background())
}

func (e *Engine) Merge(current any, incoming any, observedAt time.Time) ([]byte, error) {
	e.observedAt = observedAt.UTC().Format(time.RFC3339Nano)
	defer func() {
		e.observedAt = ""
	}()
	currentValue, err := e.encodeValue(current)
	if err != nil {
		return nil, err
	}
	incomingValue, err := e.encodeValue(incoming)
	if err != nil {
		return nil, err
	}
	arguments := []vm.Value{currentValue, incomingValue}

	ctx, cancel := context.WithTimeout(context.Background(), mergeTimeout)
	defer cancel()
	e.luaVM.SetContext(ctx)
	defer e.luaVM.SetContext(context.Background())
	results, err := e.luaVM.ProtectedCall(e.merge, arguments)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("merge returned %d values, expected one", len(results))
	}
	decoded, err := e.bridge.luaToGo(results[0], make(map[*vm.Table]bool))
	if err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func (e *Engine) encodeValue(value any) (vm.Value, error) {
	if value == nil {
		return vm.Nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return vm.Nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return vm.Nil, err
	}
	if decoded == nil {
		return vm.Nil, nil
	}
	return e.bridge.goToLua(decoded)
}
